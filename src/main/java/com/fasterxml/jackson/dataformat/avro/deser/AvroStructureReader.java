package com.fasterxml.jackson.dataformat.avro.deser;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonToken;

/**
 * Base class for handlers for Avro structured types (or, in case of
 * root values, wrapped scalar values).
 */
public abstract class AvroStructureReader
{
    /*
    /**********************************************************************
    /* Reader API
    /**********************************************************************
     */

    /**
     * Method for creating actual instance to use for reading (initial
     * instance constructed is so-called blue print).
     */
    public abstract AvroStructureReader newReader(BinaryDecoder decoder, AvroParserImpl parser);

    /**
     * Method for reading next token; returns null if reader can not read
     * more entries.
     */
    public abstract JsonToken nextToken() throws IOException;
    
    /*
    /**********************************************************************
    /* Factory methods
    /**********************************************************************
     */
    
    /**
     * Method for creating a reader instance for specified type.
     */
    public static AvroStructureReader createReader(Schema schema)
    {
        switch (schema.getType()) {
        case ARRAY:
            return createArrayReader(schema);
        case MAP: 
            return createMapReader(schema);
        case RECORD:
            return createRecordReader(schema);
        case UNION:
            return createUnionReader(schema);
        default:
            // for other types, we need wrappers
            return new ScalarDecoderWrapper(AvroScalarReader.createDecoder(schema));
        }
    }

    private static AvroStructureReader createArrayReader(Schema schema)
    {
        Schema elementType = schema.getElementType();
        AvroScalarReader scalar = AvroScalarReader.createDecoder(elementType);
        if (scalar != null) {
            return new ScalarArrayReader(scalar);
        }
        return new NonScalarArrayReader(createReader(elementType));
    }

    private static AvroStructureReader createMapReader(Schema schema)
    {
        Schema elementType = schema.getElementType();
        AvroScalarReader dec = AvroScalarReader.createDecoder(elementType);
        if (dec != null) {
            return new ScalarMapReader(dec);
        }
        return new NonScalarMapReader(createReader(elementType));
    }

    private static AvroStructureReader createRecordReader(Schema schema)
    {
        final List<Schema.Field> fields = schema.getFields();
        AvroFieldReader[] fieldReaders = new AvroFieldReader[fields.size()];
        int i = 0;
        for (Schema.Field field : fields) {
            fieldReaders[i++] = createFieldReader(field);
        }
        return new RecordReader(fieldReaders);
    }
    
    private static AvroStructureReader createUnionReader(Schema schema)
    {
        final List<Schema> types = schema.getTypes();
        AvroStructureReader[] typeReaders = new AvroStructureReader[types.size()];
        int i = 0;
        for (Schema type : types) {
            typeReaders[i++] = createReader(type);
        }
        return new UnionReader(typeReaders);
    }

    private static AvroFieldReader createFieldReader(Schema.Field field) {
        return createFieldReader(field.schema());
    }

    private static AvroFieldReader createFieldReader(Schema type)
    {
        AvroScalarReader scalar = AvroScalarReader.createDecoder(type);
        if (scalar != null) {
            return new AvroFieldReader(scalar);
        }
        return new AvroFieldReader(createReader(type));
    }

    /*
    /**********************************************************************
    /* Reader implementations for Avro arrays
    /**********************************************************************
     */

    private abstract static class ArrayReader extends AvroStructureReader
    {
        protected final static int STATE_START = 0;
        protected final static int STATE_ELEMENTS = 1;
        protected final static int STATE_END = 2;
        protected final static int STATE_DONE = 3;

        protected final BinaryDecoder _decoder;
        protected final AvroParserImpl _parser;

        protected int _state;
        protected long _index;
        protected long _count;

        protected ArrayReader(BinaryDecoder decoder, AvroParserImpl parser)
        {
            _decoder = decoder;
            _parser = parser;
        }
    }
    
    private final static class ScalarArrayReader extends ArrayReader
    {
        private final AvroScalarReader _elementReader;
        
        public ScalarArrayReader(AvroScalarReader reader) {
            this(reader, null, null);
        }

        private ScalarArrayReader(AvroScalarReader reader, 
                BinaryDecoder decoder, AvroParserImpl parser) {
            super(decoder, parser);
            _elementReader = reader;
        }
        
        @Override
        public ScalarArrayReader newReader(BinaryDecoder decoder, AvroParserImpl parser) {
            return new ScalarArrayReader(_elementReader, decoder, parser);
        }

        @Override
        public JsonToken nextToken() throws IOException
        {
            switch (_state) {
            case STATE_START:
                _count = _decoder.readArrayStart();
                _state = (_count > 0) ? STATE_ELEMENTS : STATE_END;
                return JsonToken.START_ARRAY;
            case STATE_ELEMENTS:
                break;
            case STATE_END:
                _state = STATE_DONE;
                return JsonToken.END_ARRAY;
            case STATE_DONE:
                return null;
            }
            if (_index >= _count) { // need more data
                _count = _decoder.arrayNext();
                // all traversed?
                if (_count <= 0L) {
                    _state = STATE_DONE;
                    return JsonToken.END_ARRAY;
                }
            }
            ++_index;
            return _elementReader.readValue(_parser, _decoder);
        }        
    }

    private final static class NonScalarArrayReader extends ArrayReader
    {
        private final AvroStructureReader _elementReader;
        
        public NonScalarArrayReader(AvroStructureReader reader) {
            this(reader, null, null);
        }

        private NonScalarArrayReader(AvroStructureReader reader, 
                BinaryDecoder decoder, AvroParserImpl parser) {
            super(decoder, parser);
            _elementReader = reader;
        }
        
        @Override
        public NonScalarArrayReader newReader(BinaryDecoder decoder, AvroParserImpl parser) {
            return new NonScalarArrayReader(_elementReader, decoder, parser);
        }

        @Override
        public JsonToken nextToken() throws IOException
        {
            switch (_state) {
            case STATE_START:
                _count = _decoder.readArrayStart();
                _state = (_count > 0) ? STATE_ELEMENTS : STATE_END;
                return JsonToken.START_ARRAY;
            case STATE_ELEMENTS:
                break;
            case STATE_END:
                _state = STATE_DONE;
                return JsonToken.END_ARRAY;
            case STATE_DONE:
                return null;
            }
            if (_index >= _count) { // need more data
                _count = _decoder.arrayNext();
                // all traversed?
                if (_count <= 0L) {
                    _state = STATE_DONE;
                    return JsonToken.END_ARRAY;
                }
            }
            ++_index;
            _parser.setAvroContext(_elementReader);
            return _elementReader.nextToken();
        }        
    }
    
    /*
    /**********************************************************************
    /* Reader implementations for Avro maps
    /**********************************************************************
     */

    private final static class ScalarMapReader extends AvroStructureReader
    {
        private final AvroScalarReader _valueReader;
        private final BinaryDecoder _decoder;
        private final AvroParserImpl _parser;
        
        public ScalarMapReader(AvroScalarReader reader) {
            this(reader, null, null);
        }

        private ScalarMapReader(AvroScalarReader reader, 
                BinaryDecoder decoder, AvroParserImpl parser) {
            _valueReader = reader;
            _decoder = decoder;
            _parser = parser;
        }
        
        @Override
        public ScalarMapReader newReader(BinaryDecoder decoder, AvroParserImpl parser) {
            return new ScalarMapReader(_valueReader, decoder, parser);
        }

        @Override
        public JsonToken nextToken() throws IOException
        {
        }        
    }

    private final static class NonScalarMapReader extends AvroStructureReader
    {
        private final AvroStructureReader _valueReader;
        private final BinaryDecoder _decoder;
        private final AvroParserImpl _parser;
        
        public NonScalarMapReader(AvroStructureReader reader) {
            this(reader, null, null);
        }

        private NonScalarMapReader(AvroStructureReader reader, 
                BinaryDecoder decoder, AvroParserImpl parser) {
            _valueReader = reader;
            _decoder = decoder;
            _parser = parser;
        }
        
        @Override
        public NonScalarMapReader newReader(BinaryDecoder decoder, AvroParserImpl parser) {
            return new NonScalarMapReader(_valueReader, decoder, parser);
        }

        @Override
        public JsonToken nextToken() throws IOException
        {
        }        
    }
    
    /*
    /**********************************************************************
    /* Reader implementation for Avro records
    /**********************************************************************
     */

    private final static class RecordReader extends AvroStructureReader
    {
        private final AvroFieldReader[] _fieldReaders;
        private final BinaryDecoder _decoder;
        private final AvroParserImpl _parser;
        
        public RecordReader(AvroFieldReader[] fieldReaders) {
            this(fieldReaders, null, null);
        }

        private RecordReader(AvroFieldReader[] fieldReaders,
                BinaryDecoder decoder, AvroParserImpl parser) {
            _fieldReaders = fieldReaders;
            _decoder = decoder;
            _parser = parser;
        }
        
        @Override
        public RecordReader newReader(BinaryDecoder decoder, AvroParserImpl parser) {
            return new RecordReader(_fieldReaders, decoder, parser);
        }

        @Override
        public JsonToken nextToken() throws IOException
        {
        }        
    }
    
    /*
    /**********************************************************************
    /* Reader implementation for Avro unions with non-scalar types
    /**********************************************************************
     */

    /**
     * Reader used in cases where union contains at least one non-scalar
     * type.
     */
    private final static class UnionReader extends AvroStructureReader
    {
        private final AvroStructureReader[] _memberReaders;
        private final BinaryDecoder _decoder;
        private final AvroParserImpl _parser;

        private AvroStructureReader _currentReader;
        
        public UnionReader(AvroStructureReader[] memberReaders) {
            this(memberReaders, null, null);
        }

        private UnionReader(AvroStructureReader[] memberReaders,
                BinaryDecoder decoder, AvroParserImpl parser) {
            _memberReaders = memberReaders;
            _decoder = decoder;
            _parser = parser;
        }
        
        @Override
        public UnionReader newReader(BinaryDecoder decoder, AvroParserImpl parser) {
            return new UnionReader(_memberReaders, decoder, parser);
        }

        @Override
        public JsonToken nextToken() throws IOException
        {
            if (_currentReader == null) {
                int index = _decoder.readIndex();
                if (index < 0 || index >= _memberReaders.length) {
                    throw new JsonParseException("Invalid index ("+index+"); union only has "
                            +_memberReaders.length+" types",
                            _parser.getCurrentLocation());
                }
                // important: remember to create new instance
                _currentReader = _memberReaders[index].newReader(_decoder, _parser);
            }
            return _currentReader.nextToken();
        }
    }

    private final static class ScalarDecoderWrapper extends AvroStructureReader
    {
        private final AvroScalarReader _wrappedReader;
        private final BinaryDecoder _decoder;
        private final AvroParserImpl _parser;

        protected boolean _completed;
        
        public ScalarDecoderWrapper(AvroScalarReader wrappedReader) {
            this(wrappedReader, null, null);
        }

        private ScalarDecoderWrapper(AvroScalarReader wrappedReader,
                BinaryDecoder decoder, AvroParserImpl parser) {
            _wrappedReader = wrappedReader;
            _decoder = decoder;
            _parser = parser;
        }

        @Override
        public ScalarDecoderWrapper newReader(BinaryDecoder decoder, AvroParserImpl parser) {
            return new ScalarDecoderWrapper(_wrappedReader, decoder, parser);
        }

        @Override
        public JsonToken nextToken() throws IOException
        {
            if (_completed) {
                return null;
            }
            _completed = true;
            return _wrappedReader.readValue(_parser, _decoder);
        }
    
    }
}
