package com.fasterxml.jackson.dataformat.avro.deser;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonToken;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;

public abstract class AvroScalarReader
{
    protected final static AvroScalarReader DECODER_BOOLEAN = new BooleanReader();
    protected final static AvroScalarReader DECODER_BYTES = new BytesReader();
    protected final static AvroScalarReader DECODER_DOUBLE = new DoubleReader();
    protected final static AvroScalarReader DECODER_FLOAT = new FloatReader();
    protected final static AvroScalarReader DECODER_INT = new IntReader();
    protected final static AvroScalarReader DECODER_LONG = new LongReader();
    protected final static AvroScalarReader DECODER_NULL = new NullReader();
    protected final static AvroScalarReader DECODER_STRING = new StringReader();

    public static AvroScalarReader createDecoder(Schema type)
    {
        switch (type.getType()) {
        case BOOLEAN:
            return DECODER_BOOLEAN;
        case BYTES: 
            return DECODER_BYTES;
        case DOUBLE: 
            return DECODER_DOUBLE;
        case ENUM: 
            return new EnumDecoder(type);
        case FIXED: 
            return new FixedDecoder(type);
        case FLOAT: 
            return DECODER_FLOAT;
        case INT:
            return DECODER_INT;
        case LONG: 
            return DECODER_LONG;
        case NULL: 
            return DECODER_NULL;
        case STRING: 
            return DECODER_STRING;
        case UNION:
            /* Union is a "scalar union" if all the alternative types
             * are scalar. One common type is that of "nullable" one,
             * but general handling should work just fine.
             */
            List<Schema> types = type.getTypes();
            {
                AvroScalarReader[] readers = new AvroScalarReader[types.size()];
                int i = 0;
                for (Schema schema : types) {
                    AvroScalarReader reader = createDecoder(schema);
                    if (reader == null) { // non-scalar; no go
                        return null;
                    }
                    readers[i++] = reader;
                }
                return new ScalarUnionReader(readers);
            }
        case ARRAY: // ok to call just can't handle
        case MAP:
        case RECORD:
            return null;
        }
        // but others are not recognized
        throw new IllegalStateException("Unrecognized Avro Schema type: "+type.getType());

    }

    protected abstract JsonToken readValue(AvroParserImpl parser, BinaryDecoder decoder)
        throws IOException;

    /*
    /**********************************************************************
    /* Scalar lead value decoder implementations
    /**********************************************************************
     */

    protected final static class ScalarUnionReader
        extends AvroScalarReader
    {
        public final AvroScalarReader[] _readers;

        public ScalarUnionReader(AvroScalarReader[] readers) {
            _readers = readers;
        }
        
        @Override
        protected JsonToken readValue(AvroParserImpl parser, BinaryDecoder decoder)
            throws IOException
        {
            int index = decoder.readIndex();
            if (index < 0 || index >= _readers.length) {
                throw new JsonParseException("Invalid index ("+index+"); union only has "
                        +_readers.length+" types",
                        parser.getCurrentLocation());
            }
            return _readers[index].readValue(parser, decoder);
        }
    }
    
    protected final static class BooleanReader
        extends AvroScalarReader
    {
        @Override
        protected JsonToken readValue(AvroParserImpl parser, BinaryDecoder decoder)
            throws IOException {
            return decoder.readBoolean() ? JsonToken.VALUE_TRUE : JsonToken.VALUE_FALSE;
        }
    }
    
    protected final static class BytesReader
        extends AvroScalarReader
    {
        @Override public JsonToken readValue(AvroParserImpl parser, BinaryDecoder decoder)
            throws IOException
        {
            ByteBuffer bb = parser.borrowByteBuffer();
            decoder.readBytes(bb);
            return parser.setBytes(bb);
        }
    }
    
    protected final static class DoubleReader
        extends AvroScalarReader
    {
        @Override public JsonToken readValue(AvroParserImpl parser, BinaryDecoder decoder)
            throws IOException
        {
            return parser.setNumber(decoder.readDouble());
        }
    }
    
    protected final static class FloatReader
        extends AvroScalarReader
    {
        @Override public JsonToken readValue(AvroParserImpl parser, BinaryDecoder decoder)
                throws IOException
        {
            return parser.setNumber(decoder.readFloat());
        }
    }
    
    protected final static class IntReader
        extends AvroScalarReader
    {
        @Override
        public JsonToken readValue(AvroParserImpl parser, BinaryDecoder decoder)
                throws IOException
        {
            return parser.setNumber(decoder.readInt());
        }
    }
    
    protected final static class LongReader
        extends AvroScalarReader
    {
        @Override
        public JsonToken readValue(AvroParserImpl parser, BinaryDecoder decoder)
            throws IOException
        {
            return parser.setNumber(decoder.readLong());
        }
    }
    
    protected final static class NullReader
        extends AvroScalarReader
    {
        @Override public JsonToken readValue(AvroParserImpl parser, BinaryDecoder decoder) {
            return JsonToken.VALUE_NULL;
        }
    }
    
    protected final static class StringReader
        extends AvroScalarReader
    {
        @Override
        public JsonToken readValue(AvroParserImpl parser, BinaryDecoder decoder)
            throws IOException
        {
            return parser.setString(decoder.readString());
        }
    }

    protected final static class EnumDecoder
        extends AvroScalarReader
    {
        protected final String[] _values;
        
        public EnumDecoder(Schema schema)
        {
            List<String> v = schema.getEnumSymbols();
            _values = v.toArray(new String[v.size()]);
        }
        
        @Override
        public JsonToken readValue(AvroParserImpl parser, BinaryDecoder decoder)
            throws IOException
        {
            int index = decoder.readEnum();
            if (index < 0 || index >= _values.length) {
                throw new IOException("Illegal Enum index ("+index+"): only "+_values.length+" entries");
            }
            return parser.setString(_values[index]);
        }
    }

    protected final static class FixedDecoder
        extends AvroScalarReader
    {
        protected final int _size;
        
        public FixedDecoder(Schema schema)
        {
            _size = schema.getFixedSize();
        }
        
        @Override
        public JsonToken readValue(AvroParserImpl parser, BinaryDecoder decoder)
            throws IOException
        {
            byte[] data = new byte[_size];
            decoder.readFixed(data);
            return parser.setBytes(data);
        }
    }
}
