package com.fasterxml.jackson.dataformat.avro.deser;

import java.io.IOException;

import org.apache.avro.io.BinaryDecoder;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.dataformat.avro.AvroReadContext;

abstract class ArrayReader extends AvroStructureReader
{
    protected final static int STATE_START = 0;
    protected final static int STATE_ELEMENTS = 1;
    protected final static int STATE_END = 2;
    protected final static int STATE_DONE = 3;

    protected final BinaryDecoder _decoder;
    protected final AvroParserImpl _parser;

    protected int _state;
    protected long _index;
    protected long _count;

    protected String _currentName;
    
    protected ArrayReader(AvroReadContext parent,
            AvroParserImpl parser, BinaryDecoder decoder)
    {
        super(parent, TYPE_ARRAY);
        _parser = parser;
        _decoder = decoder;
    }

    public static ArrayReader scalar(AvroScalarReader reader) {
        return new Scalar(reader);
    }

    public static ArrayReader nonScalar(AvroStructureReader reader) {
        return new NonScalar(reader);
    }

    @Override
    public String getCurrentName() {
        if (_currentName == null) {
            _currentName = _parent.getCurrentName();
        }
        return _currentName;
    }

    @Override
    protected void appendDesc(StringBuilder sb) {
        sb.append('[');
        sb.append(getCurrentIndex());
        sb.append(']');
    }
    
    /*
    /**********************************************************************
    /* Reader implementations for Avro arrays
    /**********************************************************************
     */

    private final static class Scalar extends ArrayReader
    {
        private final AvroScalarReader _elementReader;
        
        public Scalar(AvroScalarReader reader) {
            this(null, reader, null, null);
        }

        private Scalar(AvroReadContext parent,
                AvroScalarReader reader, 
                AvroParserImpl parser, BinaryDecoder decoder) {
            super(parent, parser, decoder);
            _elementReader = reader;
        }
        
        @Override
        public Scalar newReader(AvroParserImpl parser, BinaryDecoder decoder) {
            return new Scalar(_parent, _elementReader, parser, decoder);
        }

        @Override
        public JsonToken nextToken() throws IOException
        {
            switch (_state) {
            case STATE_START:
                _parser.setAvroContext(this);
                _count = _decoder.readArrayStart();
                _state = (_count > 0) ? STATE_ELEMENTS : STATE_END;
                return JsonToken.START_ARRAY;
            case STATE_ELEMENTS:
                break;
            case STATE_END:
                _state = STATE_DONE;
                _parser.setAvroContext(getParent());
                return JsonToken.END_ARRAY;
            case STATE_DONE:
            default:
                throwIllegalState(_state);
                return null;
            }
            if (_index >= _count) { // need more data
                _count = _decoder.arrayNext();
                // all traversed?
                if (_count <= 0L) {
                    _state = STATE_DONE;
                    return JsonToken.END_ARRAY;
                }
            }
            ++_index;
            return _elementReader.readValue(_parser, _decoder);
        }
    }

    private final static class NonScalar extends ArrayReader
    {
        private final AvroStructureReader _elementReader;
        
        public NonScalar(AvroStructureReader reader) {
            this(null, reader, null, null);
        }

        private NonScalar(AvroReadContext parent,
                AvroStructureReader reader, 
                AvroParserImpl parser, BinaryDecoder decoder) {
            super(parent, parser, decoder);
            _elementReader = reader;
        }
        
        @Override
        public NonScalar newReader(AvroParserImpl parser, BinaryDecoder decoder) {
            return new NonScalar(_parent, _elementReader, parser, decoder);
        }

        @Override
        public JsonToken nextToken() throws IOException
        {
            switch (_state) {
            case STATE_START:
                _parser.setAvroContext(this);
                _count = _decoder.readArrayStart();
                _state = (_count > 0) ? STATE_ELEMENTS : STATE_END;
                return JsonToken.START_ARRAY;
            case STATE_ELEMENTS:
                break;
            case STATE_END:
                _state = STATE_DONE;
                _parser.setAvroContext(getParent());
                return JsonToken.END_ARRAY;
            case STATE_DONE:
            default:
                throwIllegalState(_state);
            }
            if (_index >= _count) { // need more data
                _count = _decoder.arrayNext();
                // all traversed?
                if (_count <= 0L) {
                    _state = STATE_DONE;
                    return JsonToken.END_ARRAY;
                }
            }
            ++_index;
            AvroStructureReader r = _elementReader.newReader(_parser, _decoder);
            _parser.setAvroContext(r);
            return r.nextToken();
        }
    }
}