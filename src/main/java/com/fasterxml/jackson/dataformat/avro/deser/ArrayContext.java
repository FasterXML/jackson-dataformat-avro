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
    protected final static int STATE_START = 0;
    protected final static int STATE_ELEMENTS = 1;
    protected final static int STATE_END = 2;
    protected final static int STATE_DONE = 3;
    
    protected final AvroParserImpl _parser;

    protected final BinaryDecoder _decoder;
    
    /**
     * Number of elements in current chunk of array elements,
     * if positive non-zero number; otherwise indicates end
     * of content
     */
    protected long _currentCount;
    
    protected long _index; // marker for 'return START_ARRAY'

    protected ReadContextBase _elementReader;

    protected String _currentName;

    protected int _state;
    
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
        _currentCount = _decoder.readArrayStart();
    }

    @Override
    protected boolean isStructured() { return true; }

    @Override
    public String getCurrentName() {
        if (_currentName == null) {
            _currentName = _parent.getCurrentName();
        }
        return _currentName;
    }
    
    @Override
    public JsonToken nextToken() throws IOException
    {
        switch (_state) {
        case STATE_START:
            _state = (_currentCount > 0) ? STATE_ELEMENTS : STATE_END;
            return JsonToken.START_ARRAY;
        case STATE_ELEMENTS:
            break;
        case STATE_END:
            _state = STATE_DONE;
            return JsonToken.END_ARRAY;
        case STATE_DONE:
            return null;
        }
        if (_index >= _currentCount) { // need more data
            _currentCount = _decoder.arrayNext();
            // all traversed?
            if (_currentCount <= 0L) {
                _state = STATE_DONE;
                return JsonToken.END_ARRAY;
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