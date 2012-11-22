package com.fasterxml.jackson.dataformat.avro.schema;

import org.apache.avro.Schema;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatTypes;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonArrayFormatVisitor;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatVisitable;

public class ArrayVisitor
    extends VisitorBase
    implements JsonArrayFormatVisitor
{
    protected final JavaType _type;
    
    protected final DefinedSchemas _schemas;

    protected Schema _elementSchema;
    
    public ArrayVisitor(JavaType type, DefinedSchemas schemas)
    {
        _type = type;
        _schemas = schemas;
    }
    
    @Override
    public Schema getAvroSchema() {
        if (_elementSchema == null) {
            throw new IllegalStateException("No element schema created for: "+_type);
        }
        return Schema.createArray(_elementSchema);
    }

    /*
    /**********************************************************
    /* JsonArrayFormatVisitor implementation
    /**********************************************************
     */

    @Override
    public void itemsFormat(JsonFormatVisitable visitable, JavaType type)
            throws JsonMappingException
    {
        VisitorFormatWrapperImpl wrapper = new VisitorFormatWrapperImpl(_schemas);
        visitable.acceptJsonFormatVisitor(wrapper, type);
        _elementSchema = wrapper.getAvroSchema();
    }

    @Override
    public void itemsFormat(JsonFormatTypes type) throws JsonMappingException
    {
        _elementSchema = simpleSchema(type);
    }
}
