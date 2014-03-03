package com.fasterxml.jackson.dataformat.avro.ser;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.dataformat.avro.AvroGenerator;
import com.fasterxml.jackson.dataformat.avro.AvroWriteContext;

/**
 * Alternative to {@link ObjectWriteContext} that needs to be used with
 * Avro Map datatype.
 */
public final class MapWriteContext
    extends KeyValueContext
{
    protected final Map<String,Object> _data;
    
    public MapWriteContext(AvroWriteContext parent, AvroGenerator generator, Schema schema)
    {
        super(parent, generator, schema);
        _data = new HashMap<String,Object>();
    }

    @Override
    public Object rawValue() { return _data; }

    @Override
    public final AvroWriteContext createChildArrayContext() {
        _verifyValueWrite();
        AvroWriteContext child = new ArrayWriteContext(this, _generator, _createArray(_schema.getElementType()));
        _data.put(_currentName, child.rawValue());
        return child;
    }

    @Override
    public final AvroWriteContext createChildObjectContext() throws JsonMappingException
    {
        _verifyValueWrite();
        AvroWriteContext child = _createObjectContext(_schema.getElementType());
        _data.put(_currentName, child.rawValue());
        return child;
    }

    @Override
    public void writeValue(Object value) {
        _verifyValueWrite();
        _data.put(_currentName, value);
    }
    
    protected Schema.Field _findField() {
        throw new UnsupportedOperationException("Avro Maps do not define fields");
    }
}
