package com.fasterxml.jackson.dataformat.avro.deser;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.dataformat.avro.AvroReadContext;

// Context used for Avro Arrays
final class ArrayContext extends ReadContextBase
{
    /**
     * Number of elements in current chunk of array elements,
     * if positive non-zero number; otherwise indicates end
     * of content
     */
    protected final long _currentCount;
    
    protected long _index = -1L; // marker for 'return START_ARRAY'
    
    protected ReadContextBase _child;

    /**
     * Marker to indicate whether element values are structured
     * (exposed as Arrays and Objects) or not (simple values)
     */
    protected final boolean _structuredValue;
    public ArrayContext(AvroReadContext parent,
            AvroParserImpl parser, BinaryDecoder decoder, Schema schema)
        throws IOException
    {
        super(TYPE_ARRAY, parent, parser, decoder);
        _child = createContext(schema.getElementType());
        _structuredValue = _child.isStructured();
        _currentCount = decoder.readArrayStart();
        _index = 0L;
    }

    @Override
    protected boolean isStructured() { return true; }
    
    @Override
    public JsonToken nextToken() throws IOException
    {
        return null;
    }

    @Override
    public void appendDesc(StringBuilder sb)
    {
        sb.append('[');
        sb.append(getCurrentIndex());
        sb.append(']');
    }
}