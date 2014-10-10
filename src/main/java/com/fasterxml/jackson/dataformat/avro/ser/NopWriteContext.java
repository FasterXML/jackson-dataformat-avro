package com.fasterxml.jackson.dataformat.avro.ser;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.dataformat.avro.AvroGenerator;

/**
 * Bogus {@link AvroWriteContext} used when ignoring structured output.
 */
public class NopWriteContext extends AvroWriteContext
{
    public NopWriteContext(AvroWriteContext parent, AvroGenerator generator) {
        super(TYPE_ARRAY, parent, generator, null);
    }

    @Override
    public Object rawValue() { return null; }

    @Override
    public final AvroWriteContext createChildArrayContext() throws JsonMappingException {
        return new NopWriteContext(this, _generator);
    }
    
    @Override
    public final AvroWriteContext createChildObjectContext() throws JsonMappingException {
        return new NopWriteContext(this, _generator);
    }
    
    @Override
    public void writeValue(Object value) { }

    @Override
    public void writeString(String value) { }
    
    @Override
    public void appendDesc(StringBuilder sb) {
        sb.append("(...)");
    }
}
