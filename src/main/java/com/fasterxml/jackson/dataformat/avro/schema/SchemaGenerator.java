package com.fasterxml.jackson.dataformat.avro.schema;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsonFormatVisitors.*;

public class SchemaGenerator
    implements JsonFormatVisitorWrapper
{
    protected SerializerProvider _provider;
    
    protected final DefinedSchemas _schemas;

    /*
    /**********************************************************************
    /* Construction
    /**********************************************************************
     */
    
    public SchemaGenerator()
    {
        _schemas = new DefinedSchemas();
    }
    
    @Override
    public SerializerProvider getProvider() {
        return _provider;
    }

    @Override
    public void setProvider(SerializerProvider provider) {
        _schemas.setProvider(provider);
        _provider = provider;
    }

    /*
    /**********************************************************************
    /* Callbacks
    /**********************************************************************
     */
    
    @Override
    public JsonObjectFormatVisitor expectObjectFormat(JavaType convertedType) {
        return new RecordVisitor(convertedType, _schemas);
    }

    @Override
    public JsonArrayFormatVisitor expectArrayFormat(JavaType convertedType) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public JsonStringFormatVisitor expectStringFormat(JavaType convertedType) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public JsonNumberFormatVisitor expectNumberFormat(JavaType convertedType) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public JsonIntegerFormatVisitor expectIntegerFormat(JavaType convertedType) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public JsonBooleanFormatVisitor expectBooleanFormat(JavaType convertedType) throws JsonMappingException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public JsonNullFormatVisitor expectNullFormat(JavaType convertedType) throws JsonMappingException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public JsonAnyFormatVisitor expectAnyFormat(JavaType convertedType) throws JsonMappingException {
        // could theoretically create union of all possible types but...
        return _throwUnsupported();
    }

    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */

    protected <T> T _throwUnsupported() {
        throw new UnsupportedOperationException("Format variation not supported");
    }
}
