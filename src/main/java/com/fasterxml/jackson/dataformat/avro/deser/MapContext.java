package com.fasterxml.jackson.dataformat.avro.deser;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.dataformat.avro.AvroReadContext;

/**
 * Context implementation for reading Avro Map containers.
 */
public class MapContext extends ReadContextBase
{
    protected final Schema _schema;

    protected final List<Schema.Field> _fields;

    protected Schema.Field _currentField;

    protected String _currentName;
    
    protected final int _fieldCount;

    protected int _fieldIndex = -1;
    
    public MapContext(AvroReadContext parent,
            AvroParserImpl parser, Schema schema)
        throws IOException
    {
        super(TYPE_OBJECT, parent, parser);
        _schema = schema;
        _fields = schema.getFields();
        _fieldCount = _fields.size();
    }

    @Override
    public String getCurrentName() {
        return _currentName;
    }

    @Override
    protected boolean isStructured() { return true; }
    
    @Override
    public JsonToken nextToken(BinaryDecoder decoder) throws IOException
    {
        if (_fieldIndex < 0) {
            _fieldIndex = 0;
            return JsonToken.START_OBJECT;
        }
        Schema.Field curr = _currentField;
        if (curr == null) {
            // at the end (or after)?
            if (_fieldIndex >= _fieldCount) {
                if (_fieldIndex == _fieldCount) {
                    ++_fieldIndex;
                    return JsonToken.END_OBJECT;
                }
                return null;
            }
            curr = _fields.get(_fieldIndex);
            _currentField = curr;
            _currentName = curr.name();
            return JsonToken.FIELD_NAME;
        }
        ++_fieldIndex;
        ReadContextBase child = createContext(curr.schema());
        if (child.isStructured()) {
            _parser.setAvroContext(child);
            return child.nextToken(decoder);
        }
        return child.nextToken(decoder);
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