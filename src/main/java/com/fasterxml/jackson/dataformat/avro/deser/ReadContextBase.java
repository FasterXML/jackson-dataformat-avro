package com.fasterxml.jackson.dataformat.avro.deser;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.dataformat.avro.*;

abstract class ReadContextBase
    extends AvroReadContext
{
    protected final AvroParserImpl _parser;
    
    protected final BinaryDecoder _decoder;
    
    protected ReadContextBase(int type, AvroReadContext parent,
            AvroParserImpl parser, BinaryDecoder decoder)
    {
        super(parent);
        _type = type;
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
            return new ArrayContext(this, _parser, _decoder, schema);
        case BOOLEAN:
            return DECODER_BOOLEAN;
        case BYTES: 
            return DECODER_BYTES;
        case DOUBLE: 
            return DECODER_DOUBLE;
        case ENUM: 
            // !!! TODO
            break;
        case FIXED: 
            // !!! TODO
            break;
        case FLOAT: 
            return DECODER_FLOAT;
        case INT:
            return DECODER_INT;
        case LONG: 
            return DECODER_LONG;
        case MAP: 
            return new MapContext(this, _parser, _decoder, schema);
        case NULL: 
            return DECODER_NULL;
        case RECORD:
            return new RecordContext(this, _parser, _decoder, schema);
        case STRING: 
            return DECODER_STRING;
        case UNION:
            break;
        }
        */
        throw new IllegalStateException("Unrecognized Avro Schema type: "+schema.getType());
    } 

    protected abstract boolean isStructured();

    /**
     * Method only to be called on instances that return <code>false</code>
     * for {@link #isStructured}.
     */
    protected JsonToken readValue(AvroParserImpl parser, BinaryDecoder decoder)
            throws IOException
    {
        return _throwUnsupported();
    }

    protected <T> T _throwUnsupported() {
        throw new IllegalStateException("Can not call on "+getClass().getName());
    }
    
    /*
    /**********************************************************
    /* Simple leaf-value context implementations
    /**********************************************************
     */

    /**
     * Base class for simple scalar (non-structured) value
     * context implementations. These contexts are never
     * assigned to parser, and do not create a new scope.
     */
    protected abstract static class ScalarValueContext
        extends ReadContextBase
    {
        protected ScalarValueContext() {
            // no real type, not exposed to calling app, nor linked
            super(0, null, null, null);
        }

        @Override
        protected final boolean isStructured() {
            return false;
        }

        @Override
        protected void appendDesc(StringBuilder sb) {
            sb.append("?");
        }

        @Override public final JsonToken nextToken() throws IOException {
            return _throwUnsupported();
        }

        @Override
        protected abstract JsonToken readValue(AvroParserImpl parser, BinaryDecoder decoder)
            throws IOException;
    }


}
