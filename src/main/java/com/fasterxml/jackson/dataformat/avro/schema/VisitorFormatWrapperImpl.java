package com.fasterxml.jackson.dataformat.avro.schema;

import org.apache.avro.Schema;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonAnyFormatVisitor;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonArrayFormatVisitor;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonBooleanFormatVisitor;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatVisitorWrapper;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonIntegerFormatVisitor;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonNullFormatVisitor;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonNumberFormatVisitor;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonObjectFormatVisitor;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonStringFormatVisitor;

public class VisitorFormatWrapperImpl
    implements JsonFormatVisitorWrapper
{
    protected SerializerProvider _provider;
    
    protected final DefinedSchemas _schemas;

    protected VisitorBase _visitor;
    
    /*
    /**********************************************************************
    /* Construction
    /**********************************************************************
     */
    
    public VisitorFormatWrapperImpl(DefinedSchemas schemas)
    {
        _schemas = schemas;
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
    /* Extended API
    /**********************************************************************
     */

    public Schema getGeneratedSchema() {
        if (_visitor == null) {
            throw new IllegalStateException("No visit methods called: no schema generated");
        }
        return _visitor.getAvroSchema();
    }
    
    /*
    /**********************************************************************
    /* Callbacks
    /**********************************************************************
     */
    
    @Override
    public JsonObjectFormatVisitor expectObjectFormat(JavaType convertedType) {
        RecordVisitor v = new RecordVisitor(convertedType, _schemas);
        _visitor = v;
        return v;
    }

    @Override
    public JsonArrayFormatVisitor expectArrayFormat(JavaType convertedType) {
        ArrayVisitor v = new ArrayVisitor(convertedType, _schemas);
        _visitor = v;
        return v;
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
    public JsonBooleanFormatVisitor expectBooleanFormat(JavaType convertedType) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public JsonNullFormatVisitor expectNullFormat(JavaType convertedType) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public JsonAnyFormatVisitor expectAnyFormat(JavaType convertedType) {
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
