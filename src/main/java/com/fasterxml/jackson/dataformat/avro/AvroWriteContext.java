package com.fasterxml.jackson.dataformat.avro;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.*;
import org.apache.avro.io.BinaryEncoder;

import com.fasterxml.jackson.core.JsonStreamContext;

public abstract class AvroWriteContext
    extends JsonStreamContext
{
    protected final AvroWriteContext _parent;
    
    protected final AvroGenerator _generator;
    
    protected final Schema _schema;
    
    /*
    /**********************************************************
    /* Life-cycle
    /**********************************************************
     */
    
    protected AvroWriteContext(int type, AvroWriteContext parent,
            AvroGenerator generator, Schema schema)
    {
        super();
        _type = type;
        _parent = parent;
        _generator = generator;
        _schema = schema;
    }
    
    // // // Factory methods
    
    public static AvroWriteContext createRootContext(AvroGenerator generator, Schema schema) {
        return new RootContext(generator, schema);
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
    
    @Override
    public final AvroWriteContext getParent() { return _parent; }
    
    @Override
    public String getCurrentName() { return null; }

    /**
     * Method that writer is to call before it writes a field name.
     *
     * @return True for Object (record) context; false for others
     */
    public boolean writeFieldName(String name) { return false; }

    public abstract void writeValue(Object value);

    public void complete(BinaryEncoder encoder) throws IOException {
        throw new IllegalStateException("Can not be called on "+getClass().getName());
    }
    
    public boolean canClose() { return true; }
    
    protected GenericArray<Object> _array(Schema schema) {
        return new GenericData.Array<Object>(8, schema);
    }

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
            super(TYPE_ROOT, null, null, null);
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
            throw new IllegalStateException("Can not write Avro output without specifying Schema");
        }
    }
    
    private final static class RootContext
        extends AvroWriteContext
    {
        /**
         * We need to keep reference to the root value here.
         */
        protected GenericContainer _rootValue;
        
        protected RootContext(AvroGenerator generator, Schema schema) {
            super(TYPE_ROOT, null, generator, schema);
        }
        
        @Override
        public final AvroWriteContext createChildArrayContext()
        {
            // verify that root type is array (or compatible)
            switch (_schema.getType()) {
            case ARRAY:
            case UNION: // maybe
                break;
            default:
                throw new IllegalStateException("Can not write START_ARRAY; schema type is "
                        +_schema.getType());
            }
            GenericArray<Object> arr = _array(_schema);
            _rootValue = arr;
            return new ArrayContext(this, _generator, arr);
        }
        
        @Override
        public final AvroWriteContext createChildObjectContext()
        {
            // verify that root type is record (or compatible)
            switch (_schema.getType()) {
            case RECORD:
            case UNION: // maybe
                break;
            default:
                throw new IllegalStateException("Can not write START_OBJECT; schema type is "
                        +_schema.getType());
            }
            GenericRecord rec = new GenericData.Record(_schema);
            _rootValue = rec;
            return new ObjectContext(this, _generator, rec);
        }

        @Override
        public void writeValue(Object value) {
            throw new IllegalStateException("Can not write values directly in root context, outside of Records/Arrays");
        }

        @Override
        public void complete(BinaryEncoder encoder) throws IOException
        {
            new GenericDatumWriter<GenericContainer>(_schema).write(_rootValue, encoder);
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
        
        protected ObjectContext(AvroWriteContext parent, AvroGenerator generator,
                GenericRecord record)
        {
            super(TYPE_OBJECT, parent, generator, record.getSchema());
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
            GenericArray<Object> arr = _array(_findField().schema());
            _record.put(_currentName, arr);
            return new ArrayContext(this, _generator, arr);
        }
        
        @Override
        public final AvroWriteContext createChildObjectContext()
        {
            _verifyValueWrite();
            GenericRecord ob = new GenericData.Record(_findField().schema());
            _record.put(_currentName, ob);
            return new ObjectContext(this, _generator, ob);
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
        
        protected ArrayContext(AvroWriteContext parent, AvroGenerator generator,
                GenericArray<Object> array)
        {
            super(TYPE_ARRAY, parent, generator, array.getSchema());
            _array = array;
        }
        
        @Override
        public final AvroWriteContext createChildArrayContext()
        {
            GenericArray<Object> arr = _array(_schema.getElementType()); 
            _array.add(arr);
            return new ArrayContext(this, _generator, arr);
        }
        
        @Override
        public final AvroWriteContext createChildObjectContext() {
            GenericRecord ob = new GenericData.Record(_schema.getElementType());
            _array.add(ob);
            return new ObjectContext(this, _generator, ob);
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
