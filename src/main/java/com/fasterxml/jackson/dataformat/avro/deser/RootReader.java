package com.fasterxml.jackson.dataformat.avro.deser;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.dataformat.avro.AvroReadContext;

/**
 * Context used at root level; basically just a container
 * over a single Avro array or record
 */
public final class RootReader extends AvroReadContext
{
    protected final AvroStructureReader _child;
    
    public RootReader(AvroStructureReader child) {
        super(null);
        _type = TYPE_ROOT;
        _child = child;
    }

    public AvroReadContext getActualContext() {
        return _child;
    }
    
    @Override
    public JsonToken nextToken() throws IOException {
        return _child.nextToken();
    }
    
    @Override
    public void appendDesc(StringBuilder sb) {
        sb.append("/");
    }
}