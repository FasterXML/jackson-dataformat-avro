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
        AvroScalarReader dec = AvroScalarReader.createDecoder(elementType);
        if (dec != null) {
            return new ScalarArrayReader(dec);
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
        Schema elementType = schema.getElementType();
        AvroScalarReader dec = AvroScalarReader.createDecoder(elementType);
        if (dec != null) {
            return new ScalarRecordReader(dec);
        }
        return new NonScalarRecordReader(createReader(elementType));
    }

    private static AvroStructureReader createUnionReader(Schema schema)
    {
        return null;
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
    /* Reader implementations for Avro records
    /**********************************************************************
     */

    private final static class ScalarRecordReader extends AvroStructureReader
    {
        private final AvroScalarReader _valueReader;
        private final BinaryDecoder _decoder;
        private final AvroParserImpl _parser;
        
        public ScalarRecordReader(AvroScalarReader reader) {
            this(reader, null, null);
        }

        private ScalarRecordReader(AvroScalarReader reader, 
                BinaryDecoder decoder, AvroParserImpl parser) {
            _valueReader = reader;
            _decoder = decoder;
            _parser = parser;
        }
        
        @Override
        public ScalarRecordReader newReader(BinaryDecoder decoder, AvroParserImpl parser) {
            return new ScalarRecordReader(_valueReader, decoder, parser);
        }
        
    }

    private final static class NonScalarRecordReader extends AvroStructureReader
    {
        private final AvroStructureReader _valueReader;
        private final BinaryDecoder _decoder;
        private final AvroParserImpl _parser;
        
        public NonScalarRecordReader(AvroStructureReader reader) {
            this(reader, null, null);
        }

        private NonScalarRecordReader(AvroStructureReader reader, 
                BinaryDecoder decoder, AvroParserImpl parser) {
            _valueReader = reader;
            _decoder = decoder;
            _parser = parser;
        }
        
        @Override
        public NonScalarRecordReader newReader(BinaryDecoder decoder, AvroParserImpl parser) {
            return new NonScalarRecordReader(_valueReader, decoder, parser);
        }
        
    }
    
    /*
    /**********************************************************************
    /* Reader implementations for Avro unions
    /**********************************************************************
     */
}
