package com.fasterxml.jackson.dataformat.avro.schema;

import java.util.Set;

import org.apache.avro.Schema;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsonFormatVisitors.*;

public class VisitorFormatWrapperImpl
    implements JsonFormatVisitorWrapper
{
    protected SerializerProvider _provider;
    
    protected final DefinedSchemas _schemas;

    /**
     * Visitor used for resolving actual Schema, if structured type
     * (or one with complex configuration)
     */
    protected VisitorBase _visitor;

    /**
     * Schema for simple types that do not need a visitor.
     */
    protected Schema _valueSchema;
    
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

    public Schema getAvroSchema() {
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
        return new StringVisitor(convertedType);
    }

    @Override
    public JsonNumberFormatVisitor expectNumberFormat(JavaType convertedType) {
        _valueSchema = Schema.create(Schema.Type.DOUBLE);
        return new JsonNumberFormatVisitor() {
            @Override
            public void format(JsonValueFormat format) { }
            @Override
            public void enumTypes(Set<String> enums) { }
        };
    }

    @Override
    public JsonIntegerFormatVisitor expectIntegerFormat(JavaType convertedType) {
        _valueSchema = Schema.create(Schema.Type.DOUBLE);
        return new JsonIntegerFormatVisitor() {
            @Override
            public void format(JsonValueFormat format) { }
            @Override
            public void enumTypes(Set<String> enums) { }
        };
    }

    @Override
    public JsonBooleanFormatVisitor expectBooleanFormat(JavaType convertedType) {
        _valueSchema = Schema.create(Schema.Type.BOOLEAN);
        // We don't really need anything from there, so:
        return new JsonBooleanFormatVisitor() {
            @Override public void format(JsonValueFormat format) { }
            @Override public void enumTypes(Set<String> enums) { }
        };
    }

    @Override
    public JsonNullFormatVisitor expectNullFormat(JavaType convertedType) {
        _valueSchema = Schema.create(Schema.Type.NULL);
        return new JsonNullFormatVisitor() { };
    }

    @Override
    public JsonAnyFormatVisitor expectAnyFormat(JavaType convertedType) {
        // could theoretically create union of all possible types but...
        return _throwUnsupported("'Any' type not supported yet");
    }

    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */

    protected <T> T _throwUnsupported() {
        return _throwUnsupported("Format variation not supported");
    }
    protected <T> T _throwUnsupported(String msg) {
        throw new UnsupportedOperationException(msg);
    }
}
