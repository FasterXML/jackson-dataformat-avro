package com.fasterxml.jackson.dataformat.avro.ser;

import org.apache.avro.Schema;

import com.fasterxml.jackson.dataformat.avro.AvroGenerator;
import com.fasterxml.jackson.dataformat.avro.AvroWriteContext;

/**
 * Shared base class for both Record- and Map-backed types.
 */
abstract class KeyValueContext extends AvroWriteContext
{
    protected String _currentName;
    
    protected boolean _expectValue = false;

    protected KeyValueContext(AvroWriteContext parent, AvroGenerator generator,
            Schema schema)
    {
        super(TYPE_OBJECT, parent, generator, schema);
    }

    @Override
    public final String getCurrentName() { return _currentName; }

    @Override
    public boolean canClose() {
        return !_expectValue;
    }

    @Override
    public final boolean writeFieldName(String name)
    {
        _currentName = name;
        _expectValue = true;
        return true;
    }
    
    @Override
    public final void appendDesc(StringBuilder sb)
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

    protected final void _verifyValueWrite()
    {
        if (!_expectValue) {
            throw new IllegalStateException("Expecting FIELD_NAME, not value");
        }
        _expectValue = false;
    }
}
