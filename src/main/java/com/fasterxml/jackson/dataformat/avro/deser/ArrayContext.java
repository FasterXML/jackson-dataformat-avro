package com.fasterxml.jackson.dataformat.avro.deser;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.dataformat.avro.AvroReadContext;

/**
 * Context implementation for reading Avro Array values.
 */
final class ArrayContext extends ReadContextBase
{
    protected final AvroParserImpl _parser;

    protected final BinaryDecoder _decoder;
    
    /**
     * Number of elements in current chunk of array elements,
     * if positive non-zero number; otherwise indicates end
     * of content
     */
    protected long _currentCount = 0;
    
    protected long _index = -1L; // marker for 'return START_ARRAY'

    protected ReadContextBase _elementReader;

    /**
     * Marker to indicate whether element values are structured
     * (exposed as Arrays and Objects) or not (simple values)
     */
    protected final boolean _isValueStructured;

    public ArrayContext(AvroReadContext parent,
            AvroParserImpl parser, BinaryDecoder decoder,
            Schema schema)
        throws IOException
    {
        super(TYPE_ARRAY, parent, parser, decoder);
        _parser = parser;
        _decoder = decoder;
        _elementReader = createContext(schema.getElementType());
        _isValueStructured = _elementReader.isStructured();
    }

    @Override
    protected boolean isStructured() { return true; }
    
    @Override
    public JsonToken nextToken() throws IOException
    {
        /* Called on array:
         * 
         * 1. Initially, to return START_ARRAY
         * 
         */
        if (_index >= _currentCount) { // no data ready to be read
            // initial state, before any reads?
            if (_index < 0L) { // initial
                _currentCount = _decoder.readArrayStart();
                _index = 0L;
                return JsonToken.START_ARRAY;
            }
            // see if we can fetch more?
            if (_currentCount >= 0L) {
                _index = 0L;
                _currentCount = _decoder.arrayNext();
            }
            // all traversed?
            if (_currentCount <= 0L) {
                if (_index >= 0L) {
                    _index = -1L;
                    return JsonToken.END_ARRAY;
                }
                return null;
            }
        }
        ++_index;
        if (_isValueStructured) {
            _parser.setAvroContext(_elementReader);
            return _elementReader.nextToken();
        }
        return _elementReader.readValue(_parser, _decoder);
    }

    @Override
    public void appendDesc(StringBuilder sb)
    {
        sb.append('[');
        sb.append(getCurrentIndex());
        sb.append(']');
    }
}