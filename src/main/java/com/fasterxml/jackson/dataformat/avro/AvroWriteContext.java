package com.fasterxml.jackson.dataformat.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;

import com.fasterxml.jackson.core.JsonStreamContext;
import com.fasterxml.jackson.core.json.JsonWriteContext;

public class AvroWriteContext extends JsonStreamContext
{
    protected final AvroWriteContext _parent;
    
    /**
     * Name of the field of which value is to be parsed; only
     * used for OBJECT contexts
     */
    protected String _currentName;
    
    /*
    /**********************************************************
    /* Simple instance reuse slots
    /**********************************************************
     */
    
    protected AvroWriteContext _child = null;

    /**
     * Avro schema for current object (i.e. {@link #_avroData}.
     */
    protected Schema _avroSchema;
    
    /*
    /**********************************************************
    /* Life-cycle
    /**********************************************************
     */
    
    protected AvroWriteContext(int type, AvroWriteContext parent)
    {
        super();
        _type = type;
        _parent = parent;
        _index = -1;
    }
    
    // // // Factory methods
    
    public static AvroWriteContext createRootContext()
    {
        return new RootContext(TYPE_ROOT);
    }
    
    public final AvroWriteContext createChildArrayContext(GenericArray<Object> avroArray)
    {
        return (_child = new ArrayContext(this, avroArray));
    }
    
    public final AvroWriteContext createChildObjectContext(GenericRecord avroObject)
    {
        return (_child = new ObjectContext(this, avroObject));
    }
    
    // // // Shared API
    
    @Override
    public final AvroWriteContext getParent() { return _parent; }
    
    @Override
    public final String getCurrentName() { return _currentName; }

    // // // Stuff from JsonWriteContext

    /**
     * Method that writer is to call before it writes a field name.
     *
     * @return Index of the field entry (0-based)
     */
    public final int writeFieldName(String name)
    {
        if (_type == TYPE_OBJECT) {
            if (_currentName != null) { // just wrote a name...
                return JsonWriteContext.STATUS_EXPECT_VALUE;
            }
            _currentName = name;
            return (_index < 0) ? JsonWriteContext.STATUS_OK_AS_IS : JsonWriteContext.STATUS_OK_AFTER_COMMA;
        }
        return JsonWriteContext.STATUS_EXPECT_VALUE;
    }
    
    public final int writeValue()
    {
        // Most likely, object:
        if (_type == TYPE_OBJECT) {
            if (_currentName == null) {
                return JsonWriteContext.STATUS_EXPECT_NAME;
            }
            _currentName = null;
            ++_index;
            return JsonWriteContext.STATUS_OK_AFTER_COLON;
        }

        // Ok, array?
        if (_type == TYPE_ARRAY) {
            int ix = _index;
            ++_index;
            return (ix < 0) ? JsonWriteContext.STATUS_OK_AS_IS : JsonWriteContext.STATUS_OK_AFTER_COMMA;
        }
        
        // Nope, root context
        // No commas within root context, but need space
        ++_index;
        return (_index == 0) ? JsonWriteContext.STATUS_OK_AS_IS : JsonWriteContext.STATUS_OK_AFTER_SPACE;
    }
    
    // // // Internally used abstract methods
    
    protected final void appendDesc(StringBuilder sb)
    {
        if (_type == TYPE_OBJECT) {
            sb.append('{');
            if (_currentName != null) {
                sb.append('"');
                sb.append(_currentName);
                sb.append('"');
            } else {
                sb.append('?');
            }
            sb.append('}');
        } else if (_type == TYPE_ARRAY) {
            sb.append('[');
            sb.append(getCurrentIndex());
            sb.append(']');
        } else {
            // nah, ROOT:
            sb.append("/");
        }
    }
    
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
    /* Helper methods
    /**********************************************************
     */

    protected Schema _findSchemaForCurrentField()
    {
        if (_currentName == null) {
            throw new IllegalStateException("Internal error: missing current field name");
        }
        if (_avroSchema == null) {
            throw new IllegalStateException("Internal error: missing current schema");
        }
        Schema.Field f = _avroSchema.getField(_currentName);
        if (f == null) {
            throw new IllegalStateException("Internal error: could not find schema for field '"
                    +_currentName+"'");
        }
        return f.schema();
    }

    /*
    /**********************************************************
    /* Implementations
    /**********************************************************
     */

    private final static class RootContext
        extends AvroWriteContext
    {
        protected RootContext(int type) {
            super(TYPE_ROOT, null);
        }
    }

    private final static class ObjectContext
        extends AvroWriteContext
    {
        protected final GenericRecord _container;

        protected ObjectContext(AvroWriteContext parent,
                GenericRecord avroData)
        {
            super(TYPE_OBJECT, parent);
            _container = avroData;
        }
    }

    private final static class ArrayContext
        extends AvroWriteContext
    {
        protected final GenericArray<Object> _container;

        protected ArrayContext(AvroWriteContext parent,
                GenericArray<Object> avroData)
        {
            super(TYPE_OBJECT, parent);
            _container = avroData;
        }
    }
}
