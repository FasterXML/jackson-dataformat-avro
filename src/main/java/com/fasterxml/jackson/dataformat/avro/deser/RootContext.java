package com.fasterxml.jackson.dataformat.avro.deser;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.dataformat.avro.AvroReadContext;

/*
/**********************************************************
/* Impl classes
/**********************************************************
 */
/**
 * Context used at root level; basically just a container
 * over a single Avro array or record
 */
public final class RootContext extends ReadContextBase
{
    protected final AvroReadContext _child;
    
    public RootContext(AvroParserImpl parser,
            BinaryDecoder decoder, Schema schema) throws IOException
    {
        super(TYPE_ROOT, null, parser, decoder);
        _child = createContext(schema);
        parser.setAvroContext(_child);
    }
    
    @Override
    public JsonToken nextToken() throws IOException
    {
        // we have set child context to be the current for parser,
        // and we are only called when it ends; so should be ok
        // to simply return null to indicate end-of-content
        return null;
    }
    
    @Override
    public void appendDesc(StringBuilder sb) {
        sb.append("/");
    }
}