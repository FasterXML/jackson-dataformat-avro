package com.fasterxml.jackson.dataformat.avro.schema;

import org.apache.avro.Schema;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatVisitable;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonObjectFormatVisitor;

public class RecordVisitor
    extends VisitorBase
    implements JsonObjectFormatVisitor
{
    protected final JavaType _type;
    
    protected final DefinedSchemas _schemas;

    protected Schema _avroSchema;
    
    public RecordVisitor(JavaType type, DefinedSchemas schemas)
    {
        _type = type;
        _schemas = schemas;
        Class<?> cls = type.getRawClass();
        String name = cls.getSimpleName();
        Package pkg = cls.getPackage();
        String namespace = (pkg == null) ? "" : pkg.getName();
        _avroSchema = Schema.createRecord(name,
                "Schema for "+type,
                namespace, false);
        schemas.addSchema(type, _avroSchema);
    }
    
    @Override
    public Schema getAvroSchema() {
        return _avroSchema;
    }

    /*
    /**********************************************************
    /* JsonObjectFormatVisitor implementation
    /**********************************************************
     */
    
    @Override
    public void property(BeanProperty writer) throws JsonMappingException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void property(String name, JsonFormatVisitable handler,
            JavaType propertyTypeHint) throws JsonMappingException {
        // TODO Auto-generated method stub
        
    }

    @Override
    @Deprecated
    public void property(String name) throws JsonMappingException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void optionalProperty(BeanProperty writer)
            throws JsonMappingException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void optionalProperty(String name, JsonFormatVisitable handler,
            JavaType propertyTypeHint) throws JsonMappingException {
        // TODO Auto-generated method stub
        
    }

    @Override
    @Deprecated
    public void optionalProperty(String name) throws JsonMappingException {
        // TODO Auto-generated method stub
        
    }
}
