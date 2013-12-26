package com.fasterxml.jackson.dataformat.avro.deser;

import java.io.IOException;

import org.apache.avro.io.BinaryDecoder;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.dataformat.avro.AvroReadContext;

final class RecordReader extends AvroStructureReader
{
    protected final static int STATE_START = 0;
    protected final static int STATE_NAME = 1;
    protected final static int STATE_VALUE = 2;
    protected final static int STATE_END = 3;
    protected final static int STATE_DONE = 4;

    private final AvroFieldWrapper[] _fieldReaders;
    private final BinaryDecoder _decoder;
    private final AvroParserImpl _parser;

    private String _currentName;
    
    protected int _state;
//    protected int _index;
    protected final int _count;
    
    public RecordReader(AvroFieldWrapper[] fieldReaders) {
        this(null, fieldReaders, null, null);
    }

    private RecordReader(AvroReadContext parent,
            AvroFieldWrapper[] fieldReaders,
            BinaryDecoder decoder, AvroParserImpl parser)
    {
        super(parent, TYPE_OBJECT);
        _fieldReaders = fieldReaders;
        _decoder = decoder;
        _parser = parser;
        _count = fieldReaders.length;
    }
    
    @Override
    public RecordReader newReader(AvroReadContext parent,
            AvroParserImpl parser, BinaryDecoder decoder) {
        return new RecordReader(parent, _fieldReaders, decoder, parser);
    }

    @Override
    public String getCurrentName() { return _currentName; }

    @Override
    public JsonToken nextToken() throws IOException
    {
        switch (_state) {
        case STATE_START:
            _parser.setAvroContext(this);
            _state = (_count > 0) ? STATE_NAME : STATE_END;
            return JsonToken.START_OBJECT;
        case STATE_NAME:
            if (_index < _count) {
                _currentName = _fieldReaders[_index].getName();
                _state = STATE_VALUE;
                return JsonToken.FIELD_NAME;
            }
            // done; fall through
        case STATE_END:
            _state = STATE_DONE;
            _parser.setAvroContext(getParent());
            return JsonToken.END_OBJECT;
        case STATE_VALUE:
            break;
        case STATE_DONE:
        default:
            throwIllegalState(_state);
        }
        _state = STATE_NAME;
        AvroFieldWrapper field = _fieldReaders[_index];
        ++_index;
        return field.readValue(this, _parser, _decoder);
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