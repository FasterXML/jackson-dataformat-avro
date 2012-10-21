package com.fasterxml.jackson.dataformat.avro.deser;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;

import com.fasterxml.jackson.dataformat.avro.*;

abstract class ReadContextBase
    extends AvroReadContext
{
    protected final AvroReadContext _parent;

    protected final AvroParserImpl _parser;

    protected final BinaryDecoder _decoder;
    
    protected ReadContextBase(int type, AvroReadContext parent,
            AvroParserImpl parser, BinaryDecoder decoder)
    {
        super(parent);
        _type = type;
        _parent = parent;
        _parser = parser;
        _decoder = decoder;
    }

    /**
     * Helper method used for constructing a new context for specified
     * schema.
     * 
     * @param schema Schema that determines type of context needed
     */
    protected ReadContextBase createContext(Schema schema)
        throws IOException
    {
        /*
        switch (schema.getType()) {
        case ARRAY:
            return new Array();
        case BOOLEAN:
            break;
        case BYTES: 
            break;
        case DOUBLE: 
            break;
        case ENUM: 
            break;
        case FIXED: 
            break;
        case FLOAT: 
            break;
        case INT: 
            break;
        case LONG: 
            break;
        case MAP: 
            break;
        case NULL: 
            break;
        case RECORD:
            return new Record();
        case STRING: 
            break;
        case UNION:
            break;
        }
        */
        throw new IllegalStateException("Unrecognized Avro Schema type: "+schema.getType());
    }    

    protected boolean isStructured() {
        return false;
    }

}
