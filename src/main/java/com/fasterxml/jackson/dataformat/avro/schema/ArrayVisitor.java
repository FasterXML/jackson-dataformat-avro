package com.fasterxml.jackson.dataformat.avro.schema;

import org.apache.avro.Schema;

import java.util.*;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatTypes;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonArrayFormatVisitor;

public class ArrayVisitor
    extends VisitorBase
    implements JsonArrayFormatVisitor
{
    protected final JavaType _type;
    
    protected final DefinedSchemas _schemas;

    protected Schema _avroSchema;
    
    protected List<Schema.Field> _fields = new ArrayList<Schema.Field>();
    
    public ArrayVisitor(JavaType type, DefinedSchemas schemas)
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
        // Assumption now is that we are done, so let's assign fields
        _avroSchema.setFields(_fields);
        return _avroSchema;
    }

    /*
    /**********************************************************
    /* JsonArrayFormatVisitor implementation
    /**********************************************************
     */

    @Override
    public void itemsFormat(JavaType type) throws JsonMappingException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void itemsFormat(JsonFormatTypes type) throws JsonMappingException {
        // TODO Auto-generated method stub
        
    }
    
    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */
}
