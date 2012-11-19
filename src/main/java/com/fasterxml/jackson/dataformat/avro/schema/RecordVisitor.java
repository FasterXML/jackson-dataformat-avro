package com.fasterxml.jackson.dataformat.avro.schema;

import org.apache.avro.Schema;

import java.util.*;

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
    
    protected List<Schema.Field> _fields = new ArrayList<Schema.Field>();
    
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
        // Assumption now is that we are done, so let's assign fields
        _avroSchema.setFields(_fields);
        return _avroSchema;
    }

    /*
    /**********************************************************
    /* JsonObjectFormatVisitor implementation
    /**********************************************************
     */
    
    @Override
    public void property(BeanProperty writer) throws JsonMappingException
    {
        Schema schema = schemaForWriter(writer);
        _fields.add(new Schema.Field(writer.getName(), schema, null, null));
    }

    @Override
    public void property(String name, JsonFormatVisitable handler,
            JavaType propertyTypeHint) throws JsonMappingException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void optionalProperty(BeanProperty writer) throws JsonMappingException {
        Schema schema = schemaForWriter(writer);
        schema = unionWithNull(schema);
        _fields.add(new Schema.Field(writer.getName(), schema, null, null));
    }

    @Override
    public void optionalProperty(String name, JsonFormatVisitable handler,
            JavaType propertyTypeHint) throws JsonMappingException {
        // TODO Auto-generated method stub
    }

    @Override
    @Deprecated
    public void property(String name) throws JsonMappingException {
        _throwUnsupported();
    }
    
    @Override
    @Deprecated
    public void optionalProperty(String name) throws JsonMappingException {
        _throwUnsupported();
    }

    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */

    protected Schema schemaForWriter(BeanProperty prop)
        throws JsonMappingException
    {
        JavaType t = prop.getType();
        Schema s = _schemas.findSchema(t);
        if (s != null) {
            return s;
        }
        RecordVisitor v = new RecordVisitor(t, _schemas);
        prop.depositSchemaProperty(v);
        return v.getAvroSchema();
    }

    protected Schema unionWithNull(Schema otherSchema)
    {
        List<Schema> schemas = new ArrayList<Schema>();
        schemas.add(Schema.create(Schema.Type.NULL));
        schemas.add(otherSchema);
        return Schema.createUnion(schemas);
    }
    
    protected <T> T _throwUnsupported() {
        throw new UnsupportedOperationException("Format variation not supported");
    }
}
