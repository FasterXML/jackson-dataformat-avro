package com.fasterxml.jackson.dataformat.avro.deser;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;

/**
 * Base class for handlers for Avro structured types (or, in case of
 * root values, wrapped scalar values).
 */
public abstract class AvroStructureReader
{
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
        AvroFieldReader[] typeReaders = new AvroFieldReader[types.size()];
        int i = 0;
        for (Schema type : types) {
            typeReaders[i++] = createFieldReader(type);
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
    
    public abstract AvroStructureReader newReader(BinaryDecoder decoder, AvroParserImpl parser);

    /*
    /**********************************************************************
    /* Reader implementations for Avro arrays
    /**********************************************************************
     */

    private final static class ScalarArrayReader extends AvroStructureReader
    {
        private final AvroScalarReader _elementReader;
        private final BinaryDecoder _decoder;
        private final AvroParserImpl _parser;
        
        public ScalarArrayReader(AvroScalarReader reader) {
            this(reader, null, null);
        }

        private ScalarArrayReader(AvroScalarReader reader, 
                BinaryDecoder decoder, AvroParserImpl parser) {
            _elementReader = reader;
            _decoder = decoder;
            _parser = parser;
        }
        
        @Override
        public ScalarArrayReader newReader(BinaryDecoder decoder, AvroParserImpl parser) {
            return new ScalarArrayReader(_elementReader, decoder, parser);
        }
        
    }

    private final static class NonScalarArrayReader extends AvroStructureReader
    {
        private final AvroStructureReader _elementReader;
        private final BinaryDecoder _decoder;
        private final AvroParserImpl _parser;
        
        public NonScalarArrayReader(AvroStructureReader reader) {
            this(reader, null, null);
        }

        private NonScalarArrayReader(AvroStructureReader reader, 
                BinaryDecoder decoder, AvroParserImpl parser) {
            _elementReader = reader;
            _decoder = decoder;
            _parser = parser;
        }
        
        @Override
        public NonScalarArrayReader newReader(BinaryDecoder decoder, AvroParserImpl parser) {
            return new NonScalarArrayReader(_elementReader, decoder, parser);
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
        
    }
    
    /*
    /**********************************************************************
    /* Reader implementation for Avro unions
    /**********************************************************************
     */

    private final static class UnionReader extends AvroStructureReader
    {
        private final AvroFieldReader[] _memberReaders;
        private final BinaryDecoder _decoder;
        private final AvroParserImpl _parser;
        
        public UnionReader(AvroFieldReader[] memberReaders) {
            this(memberReaders, null, null);
        }

        private UnionReader(AvroFieldReader[] memberReaders,
                BinaryDecoder decoder, AvroParserImpl parser) {
            _memberReaders = memberReaders;
            _decoder = decoder;
            _parser = parser;
        }
        
        @Override
        public UnionReader newReader(BinaryDecoder decoder, AvroParserImpl parser) {
            return new UnionReader(_memberReaders, decoder, parser);
        }
        
    }

    private final static class ScalarDecoderWrapper extends AvroStructureReader
    {
        private final AvroScalarReader _wrappedReader;
        private final BinaryDecoder _decoder;
        private final AvroParserImpl _parser;
        
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
    }
}
