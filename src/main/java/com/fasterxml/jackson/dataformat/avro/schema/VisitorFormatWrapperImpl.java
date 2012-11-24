package com.fasterxml.jackson.dataformat.avro.schema;

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
    protected SchemaBuilder _builder;

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
        if (_builder == null) {            
            throw new IllegalStateException("No visit methods called on "+getClass().getName()
                    +": no schema generated");
        }
        return _builder.builtAvroSchema();
    }
    
    /*
    /**********************************************************************
    /* Callbacks
    /**********************************************************************
     */

    @Override
    public JsonObjectFormatVisitor expectObjectFormat(JavaType convertedType) {
        RecordVisitor v = new RecordVisitor(convertedType, _schemas);
        _builder = v;
        return v;
    }

    @Override
    public JsonMapFormatVisitor expectMapFormat(JavaType mapType) {
        MapVisitor v = new MapVisitor(mapType, _schemas);
        _builder = v;
        return v;
    }
    
    @Override
    public JsonArrayFormatVisitor expectArrayFormat(JavaType convertedType) {
        ArrayVisitor v = new ArrayVisitor(convertedType, _schemas);
        _builder = v;
        return v;
    }

    @Override
    public JsonStringFormatVisitor expectStringFormat(JavaType convertedType) {
        StringVisitor v = new StringVisitor(convertedType);
        _builder = v;
        return v;
    }

    @Override
    public JsonNumberFormatVisitor expectNumberFormat(JavaType convertedType) {
        DoubleVisitor v = new DoubleVisitor();
        _builder = v;
        return v;

        /*
        _valueSchema = Schema.create(Schema.Type.DOUBLE);
        return new JsonNumberFormatVisitor.Base() { };
        */
    }

    @Override
    public JsonIntegerFormatVisitor expectIntegerFormat(JavaType convertedType) {
        IntegerVisitor v = new IntegerVisitor();
        _builder = v;
        return v;
    }

    @Override
    public JsonBooleanFormatVisitor expectBooleanFormat(JavaType convertedType) {
        _valueSchema = Schema.create(Schema.Type.BOOLEAN);
        // We don't really need anything from there (should be ok to return null...)
        return new JsonBooleanFormatVisitor.Base() { };
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
