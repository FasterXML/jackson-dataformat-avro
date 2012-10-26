package com.fasterxml.jackson.dataformat.avro.deser;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;

/**
 * Base class for handlers for Avro structured types (or, in case of
 * root values, wrapped scalar values).
 */
public abstract class AvroValueReader
{
    public static AvroValueReader createReader(Schema schema)
    {
        switch (schema.getType()) {
        case ARRAY:
            return new ArrayContext(this, _parser, _decoder, schema);
        case MAP: 
            return new MapContext(this, _parser, _decoder, schema);
        case RECORD:
            return new RecordContext(this, _parser, _decoder, schema);
            /*
        case UNION:
            break;
            */
        }
        throw new IllegalStateException("Unrecognized Avro Schema type: "+schema.getType());
        return null;
    }

    public AvroValueReader newReader(BinaryDecoder decoder, AvroParserImpl parser)
    {
        
    }
}
