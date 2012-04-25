package com.fasterxml.jackson.dataformat.avro;

import com.fasterxml.jackson.core.JsonStreamContext;

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
        return new AvroWriteContext(TYPE_ROOT, null);
    }
    
    private final AvroWriteContext reset(int type) {
        _type = type;
        _index = -1;
        _currentName = null;
        return this;
    }
    
    public final AvroWriteContext createChildArrayContext()
    {
        AvroWriteContext ctxt = _child;
        if (ctxt == null) {
            _child = ctxt = new AvroWriteContext(TYPE_ARRAY, this);
            return ctxt;
        }
        return ctxt.reset(TYPE_ARRAY);
    }
    
    public final AvroWriteContext createChildObjectContext()
    {
        AvroWriteContext ctxt = _child;
        if (ctxt == null) {
            _child = ctxt = new AvroWriteContext(TYPE_OBJECT, this);
            return ctxt;
        }
        return ctxt.reset(TYPE_OBJECT);
    }
    
    // // // Shared API
    
    @Override
    public final AvroWriteContext getParent() { return _parent; }
    
    @Override
    public final String getCurrentName() { return _currentName; }
    
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
}
