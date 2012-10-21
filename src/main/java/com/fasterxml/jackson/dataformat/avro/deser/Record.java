package com.fasterxml.jackson.dataformat.avro.deser;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.dataformat.avro.AvroReadContext;

// Context used for Avro Records
final class Record extends ReadContextBase
{
    protected final Schema _schema;

    protected String _currentName;
    
    public Record(AvroReadContext parent,
            AvroParserImpl parser, BinaryDecoder decoder, Schema schema)
        throws IOException
    {
        super(TYPE_OBJECT, parent, parser, decoder);
        _schema = schema;
    }

    @Override
    public String getCurrentName() { return _currentName; }

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
        sb.append('{');
        if (_currentName != null) {
            sb.append('"');
            sb.append(_currentName);
            sb.append('"');
        } else {
            sb.append('?');
        }
        sb.append('}');
    }
}