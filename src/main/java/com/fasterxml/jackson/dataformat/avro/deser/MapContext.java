package com.fasterxml.jackson.dataformat.avro.deser;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.dataformat.avro.AvroReadContext;

/**
 * Context implementation for reading Avro Map containers.
 */
public class MapContext extends ReadContextBase
{
    protected final AvroParserImpl _parser;

    protected final BinaryDecoder _decoder;

    protected ReadContextBase _elementReader;

    /**
     * Marker to indicate whether element values are structured
     * (exposed as Arrays and Objects) or not (simple values)
     */
    protected final boolean _isValueStructured;
    
    protected long _entryCount;

    protected String _currentName;

    protected boolean _expectName = true;
    
    protected int _index = -1;
    
    public MapContext(AvroReadContext parent,
            AvroParserImpl parser, BinaryDecoder decoder,
            Schema schema)
        throws IOException
    {
        super(TYPE_OBJECT, parent, parser, decoder);
        _parser = parser;
        _decoder = decoder;
        _elementReader = createContext(schema.getElementType());
        _isValueStructured = _elementReader.isStructured();
        _entryCount = decoder.readMapStart();
    }
    
    @Override
    public String getCurrentName() {
        return _currentName;
    }

    @Override
    protected boolean isStructured() { return true; }
    
    @Override
    public JsonToken nextToken() throws IOException
    {
        if (_index < 0) {
            _index = 0;
            return JsonToken.START_OBJECT;
        }
        if (_index >= _entryCount) { // end?
            // after exhausting entries, need to indicate end
            if (_index == _entryCount) {
                ++_entryCount;
                return JsonToken.START_OBJECT;
            }
            // but after that null to know this context is done
            return null;
        }
        if (_expectName) {
            _expectName = false;
            _currentName = _decoder.readString();
            return JsonToken.FIELD_NAME;
        }
        _expectName = true;
        ++_index;
        if (_elementReader.isStructured()) {
            _parser.setAvroContext(_elementReader);
            return _elementReader.nextToken();
        }
        return _elementReader.readValue(_parser, _decoder);
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