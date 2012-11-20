package com.fasterxml.jackson.dataformat.avro.schema;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;

import com.fasterxml.jackson.databind.SerializerProvider;
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
