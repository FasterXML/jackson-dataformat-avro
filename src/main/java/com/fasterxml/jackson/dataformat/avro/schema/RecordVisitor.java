package com.fasterxml.jackson.dataformat.avro.schema;

import org.apache.avro.Schema;

import java.util.*;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatVisitable;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonObjectFormatVisitor;

public class RecordVisitor
    extends JsonObjectFormatVisitor.Base
    implements SchemaBuilder
{
    protected final JavaType _type;
    
    protected final DefinedSchemas _schemas;

    protected Schema _avroSchema;
    
    protected List<Schema.Field> _fields = new ArrayList<Schema.Field>();
    
    public RecordVisitor(JavaType type, DefinedSchemas schemas)
    {
        _type = type;
        _schemas = schemas;
        _avroSchema = Schema.createRecord(AvroSchemaHelper.getName(type),
                "Schema for "+type.toCanonical(),
                AvroSchemaHelper.getNamespace(type), false);
        schemas.addSchema(type, _avroSchema);
    }
    
    @Override
    public Schema builtAvroSchema() {
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
            JavaType type) throws JsonMappingException
    {
        VisitorFormatWrapperImpl wrapper = new VisitorFormatWrapperImpl(_schemas);
        handler.acceptJsonFormatVisitor(wrapper, type);
        Schema schema = wrapper.getAvroSchema();
        _fields.add(new Schema.Field(name, schema, null, null));
    }

    @Override
    public void optionalProperty(BeanProperty writer) throws JsonMappingException {
        Schema schema = schemaForWriter(writer);
        schema = AvroSchemaHelper.unionWithNull(schema);
        _fields.add(new Schema.Field(writer.getName(), schema, null, null));
    }

    @Override
    public void optionalProperty(String name, JsonFormatVisitable handler,
            JavaType type) throws JsonMappingException
    {
        VisitorFormatWrapperImpl wrapper = new VisitorFormatWrapperImpl(_schemas);
        handler.acceptJsonFormatVisitor(wrapper, type);
        Schema schema = wrapper.getAvroSchema();
        schema = AvroSchemaHelper.unionWithNull(schema);
        _fields.add(new Schema.Field(name, schema, null, null));
    }

    @Override
    @Deprecated
    public void property(String name) throws JsonMappingException {
        AvroSchemaHelper.throwUnsupported();
    }
    
    @Override
    @Deprecated
    public void optionalProperty(String name) throws JsonMappingException {
        AvroSchemaHelper.throwUnsupported();
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
        return v.builtAvroSchema();
    }
}
