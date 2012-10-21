package com.fasterxml.jackson.dataformat.avro;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;

import com.fasterxml.jackson.core.JsonStreamContext;

/**
 * We need to use a custom context to be able to carry along
 * Object and array records.
 */
public abstract class AvroReadContext extends JsonStreamContext
{
    protected final AvroReadContext _parent;
    
    protected final Schema _schema;

    protected final BinaryDecoder _decoder;

    /*
    /**********************************************************
    /* Instance construction
    /**********************************************************
     */

    public AvroReadContext(int type, AvroReadContext parent,
            BinaryDecoder decoder, Schema schema)
    {
        super();
        _type = type;
        _parent = parent;
        _decoder = decoder;
        _schema = schema;
    }

    // // // Factory methods

    public static AvroReadContext createRootContext(BinaryDecoder decoder,
            Schema schema) {
        return new Root(decoder, schema);
    }

    /**
     * Factory method called to get a placeholder context that is only
     * in place until actual schema is handed.
     */
    public static AvroReadContext createNullContext() {
        return Null.instance;
    }
    
    public abstract AvroReadContext createChildArrayContext();
    public abstract AvroReadContext createChildObjectContext();

    /*
    /**********************************************************
    /* Accessors
    /**********************************************************
     */

    @Override
    public String getCurrentName() { return null; }

    @Override
    public final AvroReadContext getParent() { return _parent; }

    protected abstract void appendDesc(StringBuilder sb);
    
    // // // Overridden standard methods
    
    /**
     * Overridden to provide developer writeable "JsonPath" representation
     * of the context.
     */
    @Override
    public final String toString()
    {
        StringBuilder sb = new StringBuilder(64);
        appendDesc(sb);
        return sb.toString();
    }

    /*
    /**********************************************************
    /* Impl classes
    /**********************************************************
     */

    protected static final class Null extends AvroReadContext
    {
        public final static Null instance = new Null();
        
        public Null() {
            super(TYPE_ROOT, null, null, null);
        }

        @Override
        public final AvroReadContext createChildArrayContext() {
            _reportError();
            return null;
        }
        
        @Override
        public final AvroReadContext createChildObjectContext() {
            _reportError();
            return null;
        }
        
        @Override
        public void appendDesc(StringBuilder sb) {
            sb.append("?");
        }

        protected void _reportError() {
            throw new IllegalStateException("Can not read Avro input without specifying Schema");
        }
    }
    
    protected static final class Root extends AvroReadContext
    {
        public Root(BinaryDecoder decoder, Schema schema) {
            super(TYPE_ROOT, null, decoder, schema);
        }

        @Override
        public final AvroReadContext createChildArrayContext() {
            return null;
        }
        
        @Override
        public final AvroReadContext createChildObjectContext() {
            return null;
        }
        
        @Override
        public void appendDesc(StringBuilder sb) {
            sb.append("/");
        }
    }

    protected static final class Object extends AvroReadContext
    {
        protected String _currentName;
        
        public Object(BinaryDecoder decoder, Schema schema) {
            super(TYPE_OBJECT, null, decoder, schema);
        }

        @Override
        public String getCurrentName() { return _currentName; }
        
        @Override
        public final AvroReadContext createChildArrayContext() {
            return null;
        }
        
        @Override
        public final AvroReadContext createChildObjectContext() {
            return null;
        }
        
        @Override
        public void appendDesc(StringBuilder sb)
        {
            sb.append('{');
            if (_currentName != null) {
                sb.append('"');
                sb.append(_currentName);
                sb.append('"');
            } else {
                sb.append('?');
            }
            sb.append('}');
        }
    }

    protected static final class Array extends AvroReadContext
    {
        public Array(BinaryDecoder decoder, Schema schema) {
            super(TYPE_ARRAY, null, decoder, schema);
        }

        @Override
        public final AvroReadContext createChildArrayContext() {
            return null;
        }
        
        @Override
        public final AvroReadContext createChildObjectContext() {
            return null;
        }

        @Override
        public void appendDesc(StringBuilder sb)
        {
            sb.append('[');
            sb.append(getCurrentIndex());
            sb.append(']');
        }
    }
}
