package com.fasterxml.jackson.dataformat.avro.deser;

import com.fasterxml.jackson.core.JsonToken;

import com.fasterxml.jackson.dataformat.avro.AvroReadContext;

public class EmptyContext extends AvroReadContext
{
    public final static EmptyContext instance = new EmptyContext();
    
    public EmptyContext() {
        super(null);
        _type = TYPE_ROOT;
    }

    @Override
    public JsonToken nextToken() {
        _reportError();
        return null;
    }
    
    @Override
    public void appendDesc(StringBuilder sb) {
        sb.append("?");
    }

    protected void _reportError() {
        throw new IllegalStateException("Can not read Avro input without specifying Schema");
    }
}

