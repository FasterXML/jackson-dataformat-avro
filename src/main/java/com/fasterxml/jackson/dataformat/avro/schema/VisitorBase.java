package com.fasterxml.jackson.dataformat.avro.schema;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatTypes;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatVisitorWithSerializerProvider;

public abstract class VisitorBase
    implements JsonFormatVisitorWithSerializerProvider
{
    protected SerializerProvider _provider;
    
    public abstract Schema getAvroSchema();

    @Override
    public SerializerProvider getProvider() {
        return _provider;
    }

    @Override
    public void setProvider(SerializerProvider provider) {
        _provider = provider;
    }

    /*
    /**********************************************************************
    /* Helper methods for sub-classes
    /**********************************************************************
     */

    protected static String getNamespace(JavaType type) {
        Class<?> cls = type.getRawClass();
        Package pkg = cls.getPackage();
        return (pkg == null) ? "" : pkg.getName();
    }
    
    protected static String getName(JavaType type) {
        return type.getRawClass().getSimpleName();
    }
    
    protected Schema unionWithNull(Schema otherSchema)
    {
        List<Schema> schemas = new ArrayList<Schema>();
        schemas.add(Schema.create(Schema.Type.NULL));
        schemas.add(otherSchema);
        return Schema.createUnion(schemas);
    }

    public Schema simpleSchema(JsonFormatTypes type)
    {
        switch (type) {
        case BOOLEAN:
            return Schema.create(Schema.Type.BOOLEAN);
        case INTEGER:
            return Schema.create(Schema.Type.INT);
        case NULL:
            return Schema.create(Schema.Type.NULL);
        case NUMBER:
            return Schema.create(Schema.Type.DOUBLE);
        case STRING:
            return Schema.create(Schema.Type.STRING);
        case ARRAY:
        case OBJECT:
            throw new UnsupportedOperationException("Should not try to create simple Schema for: "+type);
        case ANY: // might be able to support in future
        default:
            throw new UnsupportedOperationException("Can not create Schema for: "+type+"; not (yet) supported");
        }
    }
    
    protected <T> T _throwUnsupported() {
        throw new UnsupportedOperationException("Format variation not supported");
    }
}
