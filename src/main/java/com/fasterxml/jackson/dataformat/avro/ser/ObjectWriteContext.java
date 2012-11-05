package com.fasterxml.jackson.dataformat.avro.ser;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;

import com.fasterxml.jackson.dataformat.avro.AvroGenerator;
import com.fasterxml.jackson.dataformat.avro.AvroWriteContext;

public final class ObjectWriteContext
    extends AvroWriteContext
{
    protected final GenericRecord _record;

    protected String _currentName;
    
    protected boolean _expectValue = false;
    
    public ObjectWriteContext(AvroWriteContext parent, AvroGenerator generator,
            GenericRecord record)
    {
        super(TYPE_OBJECT, parent, generator, record.getSchema());
        _record = record;
    }

    @Override
    public final String getCurrentName() { return _currentName; }

    @Override
    public boolean canClose() {
        return !_expectValue;
    }
    
    @Override
    public final AvroWriteContext createChildArrayContext()
    {
        _verifyValueWrite();
        GenericArray<Object> arr = _createArray(_findField().schema());
        _record.put(_currentName, arr);
        return new ArrayWriteContext(this, _generator, arr);
    }
    
    @Override
    public final AvroWriteContext createChildObjectContext()
    {
        _verifyValueWrite();
        Schema.Field f = _findField();
        GenericRecord ob = _createRecord(f.schema());
        _record.put(_currentName, ob);
        return new ObjectWriteContext(this, _generator, ob);
    }
    
    @Override
    public final boolean writeFieldName(String name)
    {
        _currentName = name;
        _expectValue = true;
        return true;
    }
    
    @Override
    public void writeValue(Object value) {
        _verifyValueWrite();
        _record.put(_currentName, value);
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

    protected void _verifyValueWrite()
    {
        if (!_expectValue) {
            throw new IllegalStateException("Expecting FIELD_NAME, not value");
        }
        _expectValue = false;
    }
    
    protected Schema.Field _findField() {
        if (_currentName == null) {
            throw new IllegalStateException("No current field name");
        }
        Schema.Field f = _schema.getField(_currentName);
        if (f == null) {
            throw new IllegalStateException("No field named '"+_currentName+"'");
        }
        return f;
    }
}