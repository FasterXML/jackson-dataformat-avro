package com.fasterxml.jackson.dataformat.avro.schema;

import org.apache.avro.Schema;

import java.util.*;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatVisitable;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonObjectFormatVisitor;
import com.fasterxml.jackson.databind.ser.BeanPropertyWriter;

public class RecordVisitor
    extends JsonObjectFormatVisitor.Base
    implements SchemaBuilder
{
    protected final JavaType _type;

    protected final DefinedSchemas _schemas;

    protected Schema _avroSchema;
    
    protected List<Schema.Field> _fields = new ArrayList<Schema.Field>();
    
    public RecordVisitor(SerializerProvider p, JavaType type, DefinedSchemas schemas)
    {
        super(p);
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
        JsonSerializer<?> ser = null;

        // 23-Nov-2012, tatu: Ideally shouldn't need to do this but...
        if (prop instanceof BeanPropertyWriter) {
            BeanPropertyWriter bpw = (BeanPropertyWriter) prop;
            ser = bpw.getSerializer();
        }
        if (ser == null) {
            SerializerProvider prov = getProvider();
            if (prov == null) throw new Error();
            ser = prov.findValueSerializer(prop.getType(), prop);
        }
        VisitorFormatWrapperImpl visitor = new VisitorFormatWrapperImpl(_schemas);
        ser.acceptJsonFormatVisitor(visitor, prop.getType());
        return visitor.getAvroSchema();
    }
}
