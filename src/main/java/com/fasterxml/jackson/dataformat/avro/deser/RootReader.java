package com.fasterxml.jackson.dataformat.avro.deser;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.dataformat.avro.AvroReadContext;

/**
 * Context used at root level; basically just a container
 * over actual {@link AvroStructureReader}.
 */
public final class RootReader extends AvroReadContext
{
    public RootReader() {
        super(null);
        _type = TYPE_ROOT;
    }
    
    @Override
    public JsonToken nextToken() throws IOException {
        return null;
    }
    
    @Override
    public void appendDesc(StringBuilder sb) {
        sb.append("/");
    }
}