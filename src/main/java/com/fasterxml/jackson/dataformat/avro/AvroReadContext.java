package com.fasterxml.jackson.dataformat.avro;

import org.apache.avro.generic.GenericContainer;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonStreamContext;
import com.fasterxml.jackson.core.io.CharTypes;

/**
 * We need to use a custom context to be able to carry along
 * Object and array records.
 */
public class AvroReadContext extends JsonStreamContext
{
    // // // Configuration

    protected final AvroReadContext _parent;

    // // // Location information

    // Name of current field for Objects
    protected String _currentName;

    // note: from base class:
    //
    //protected int _index;

    // Number of entries in Object/Array
    protected int _entryCount;

    // Current array or object
    protected GenericContainer _currentContainer;
    
    /*
    /**********************************************************
    /* Simple instance reuse slots
    /**********************************************************
     */

    protected AvroReadContext _child = null;

    /*
    /**********************************************************
    /* Instance construction, reuse
    /**********************************************************
     */

    public AvroReadContext(AvroReadContext parent, int type,
            GenericContainer container, int entryCount)
    {
        super();
        _type = type;
        _parent = parent;
        _index = -1;
        _currentContainer = container;
        _entryCount = entryCount;
    }

    protected final void reset(int type)
    {
        _type = type;
        _index = -1;
        _currentName = null;
    }

    // // // Factory methods

    public static AvroReadContext createRootContext()
    {
        return new AvroReadContext(null, TYPE_ROOT, null, 0);
    }
    
    public final AvroReadContext createChildArrayContext(GenericContainer container, int entryCount)
    {
        AvroReadContext ctxt = _child;
        if (ctxt == null) {
            _child = ctxt = new AvroReadContext(this, TYPE_ARRAY, container, entryCount);
            return ctxt;
        }
        ctxt.reset(TYPE_ARRAY);
        return ctxt;
    }

    public final AvroReadContext createChildObjectContext(GenericContainer container, int entryCount)
    {
        AvroReadContext ctxt = _child;
        if (ctxt == null) {
            _child = ctxt = new AvroReadContext(this, TYPE_OBJECT, container, entryCount);
            return ctxt;
        }
        ctxt.reset(TYPE_OBJECT);
        return ctxt;
    }

    /*
    /**********************************************************
    /* Accessors
    /**********************************************************
     */

    @Override
    public final String getCurrentName() { return _currentName; }

    @Override
    public final AvroReadContext getParent() { return _parent; }

    public GenericContainer getContainer() { return _currentContainer; }
    
    /*
    /**********************************************************
    /* Extended API
    /**********************************************************
     */

    /**
     * @return Location pointing to the point where the context
     *   start marker was found
     */
    public final JsonLocation getStartLocation(Object srcRef) {
        return JsonLocation.NA;
    }

    /*
    /**********************************************************
    /* State changes
    /**********************************************************
     */

    public void setCurrentName(String name)
    {
        _currentName = name;
    }

    /*
    /**********************************************************
    /* Overridden standard methods
    /**********************************************************
     */

    /**
     * Overridden to provide developer readable "JsonPath" representation
     * of the context.
     */
    @Override
    public final String toString()
    {
        StringBuilder sb = new StringBuilder(64);
        switch (_type) {
        case TYPE_ROOT:
            sb.append("/");
            break;
        case TYPE_ARRAY:
            sb.append('[');
            sb.append(getCurrentIndex());
            sb.append(']');
            break;
        case TYPE_OBJECT:
            sb.append('{');
            if (_currentName != null) {
                sb.append('"');
                CharTypes.appendQuoted(sb, _currentName);
                sb.append('"');
            } else {
                sb.append('?');
            }
            sb.append('}');
            break;
        }
        return sb.toString();
    }
}
