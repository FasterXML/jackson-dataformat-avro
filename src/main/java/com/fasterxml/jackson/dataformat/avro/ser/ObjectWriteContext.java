package com.fasterxml.jackson.dataformat.avro.ser;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.dataformat.avro.AvroGenerator;
import com.fasterxml.jackson.dataformat.avro.AvroWriteContext;

public final class ObjectWriteContext
    extends KeyValueContext
{
    protected final GenericRecord _record;
    
    public ObjectWriteContext(AvroWriteContext parent, AvroGenerator generator,
            GenericRecord record)
    {
        super(parent, generator, record.getSchema());
        _record = record;
    }

    @Override
    public Object rawValue() { return _record; }

    @Override
    public final AvroWriteContext createChildArrayContext()
    {
        _verifyValueWrite();
        AvroWriteContext child = new ArrayWriteContext(this, _generator, _createArray(_findField().schema()));
        _record.put(_currentName, child.rawValue());
        return child;
    }

    @Override
    public final AvroWriteContext createChildObjectContext() throws JsonMappingException
    {
        _verifyValueWrite();
        Schema.Field f = _findField();
        AvroWriteContext child = _createObjectContext(f.schema());
        _record.put(_currentName, child.rawValue());
        return child;
    }

    @Override
    public void writeValue(Object value) {
        _verifyValueWrite();
        _record.put(_currentName, value);
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
