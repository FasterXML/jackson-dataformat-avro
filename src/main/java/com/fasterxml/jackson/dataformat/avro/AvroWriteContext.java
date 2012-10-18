package com.fasterxml.jackson.dataformat.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.fasterxml.jackson.core.JsonStreamContext;

public abstract class AvroWriteContext
    extends JsonStreamContext
{
    protected final AvroWriteContext _parent;
    
    /*
    /**********************************************************
    /* Simple instance reuse slots
    /**********************************************************
     */
    
    protected final Schema _schema;
    
    /*
    /**********************************************************
    /* Life-cycle
    /**********************************************************
     */
    
    protected AvroWriteContext(int type, AvroWriteContext parent,
            Schema schema)
    {
        super();
        _type = type;
        _parent = parent;
        _schema = schema;
    }
    
    // // // Factory methods
    
    public static AvroWriteContext createRootContext(Schema schema) {
        return new RootContext(schema);
    }

    /**
     * Factory method called to get a placeholder context that is only
     * in place until actual schema is handed.
     */
    public static AvroWriteContext createNullContext() {
        return NullContext.instance;
    }
    
    public abstract AvroWriteContext createChildArrayContext();
    public abstract AvroWriteContext createChildObjectContext();
    
    // // // Shared API
    
    @Override
    public final AvroWriteContext getParent() { return _parent; }
    
    @Override
    public String getCurrentName() { return null; }

    // // // Stuff from JsonWriteContext

    /**
     * Method that writer is to call before it writes a field name.
     *
     * @return True for Object (record) context; false for others
     */
    public boolean writeFieldName(String name) { return false; }

    public abstract void writeValue(Object value);

    public boolean canClose() { return true; }
    
    // // // Internally used abstract methods
    
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
    /* Implementations
    /**********************************************************
     */

    private final static class NullContext
        extends AvroWriteContext
    {
        public final static NullContext instance = new NullContext();
        
        private NullContext() {
            super(TYPE_ROOT, null, null);
        }
        
        @Override
        public final AvroWriteContext createChildArrayContext() {
            _reportError();
            return null;
        }
        
        @Override
        public final AvroWriteContext createChildObjectContext() {
            _reportError();
            return null;
        }
    
        @Override
        public void writeValue(Object value) {
            _reportError();
        }
        
        @Override
        public void appendDesc(StringBuilder sb) {
            sb.append("?");
        }

        protected void _reportError() {
            throw new IllegalStateException("Can not write output without specifying Schema");
        }
    }
    
    private final static class RootContext
        extends AvroWriteContext
    {
        protected RootContext(Schema schema) {
            super(TYPE_ROOT, null, schema);
        }
        
        @Override
        public final AvroWriteContext createChildArrayContext() {
            GenericArray<Object> arr = new GenericData.Array<Object>(8, _schema);
            return new ArrayContext(this, arr);
        }
        
        @Override
        public final AvroWriteContext createChildObjectContext() {
            return new ObjectContext(this, new GenericData.Record(_schema));
        }

        @Override
        public void writeValue(Object value) {
            throw new IllegalStateException("Can not write values directly in root context, outside of Records/Arrays");
        }
        
        @Override
        public void appendDesc(StringBuilder sb) {
            sb.append("/");
        }
    }

    private final static class ObjectContext
        extends AvroWriteContext
    {
        protected final GenericRecord _record;

        protected String _currentName;
        
        protected boolean _expectValue = false;
        
        protected ObjectContext(AvroWriteContext parent,
                GenericRecord record)
        {
            super(TYPE_OBJECT, parent, record.getSchema());
            _record = record;
        }

        @Override
        public final String getCurrentName() { return _currentName; }

        @Override
        public boolean canClose() {
            return !_expectValue;
        }
        
        @Override
        public final AvroWriteContext createChildArrayContext()
        {
            _verifyValueWrite();
            GenericArray<Object> arr = new GenericData.Array<Object>(8,
                    _findField().schema());
            return new ArrayContext(this, arr);
        }
        
        @Override
        public final AvroWriteContext createChildObjectContext()
        {
            _verifyValueWrite();
            GenericRecord ob = new GenericData.Record(_findField().schema());
            return new ObjectContext(this, ob);
        }

        @Override
        public final boolean writeFieldName(String name)
        {
            _currentName = name;
            _expectValue = true;
            return true;
        }
        
        @Override
        public void writeValue(Object value) {
            _verifyValueWrite();
            _record.put(_currentName, value);
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

        protected void _verifyValueWrite()
        {
            if (!_expectValue) {
                throw new IllegalStateException("Expecting FIELD_NAME, not value");
            }
            _expectValue = false;
        }
        
        protected Schema.Field _findField() {
            if (_currentName == null) {
                throw new IllegalStateException("No current field name");
            }
            Schema.Field f = _schema.getField(_currentName);
            if (f == null) {
                throw new IllegalStateException("No field named '"+_currentName+"'");
            }
            return f;
        }
    }

    private final static class ArrayContext
        extends AvroWriteContext
    {
        protected final GenericArray<Object> _array;
        
        protected ArrayContext(AvroWriteContext parent,
                GenericArray<Object> array)
        {
            super(TYPE_OBJECT, parent, array.getSchema());
            _array = array;
        }
        
        @Override
        public final AvroWriteContext createChildArrayContext() {
            GenericArray<Object> arr = new GenericData.Array<Object>(8,
                    _schema.getElementType());
            return new ArrayContext(this, arr);
        }
        
        @Override
        public final AvroWriteContext createChildObjectContext() {
            GenericRecord ob = new GenericData.Record(_schema.getElementType());
            return new ObjectContext(this, ob);
        }
        
        @Override
        public void writeValue(Object value) {
            _array.add(value);
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
