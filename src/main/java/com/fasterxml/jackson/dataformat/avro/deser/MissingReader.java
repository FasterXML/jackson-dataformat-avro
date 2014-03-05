package com.fasterxml.jackson.dataformat.avro.deser;

import com.fasterxml.jackson.core.JsonToken;

public class MissingReader extends AvroReadContext
{
    public final static MissingReader instance = new MissingReader();
    
    public MissingReader() {
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
}

