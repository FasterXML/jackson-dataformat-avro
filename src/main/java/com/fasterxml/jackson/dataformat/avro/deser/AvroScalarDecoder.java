package com.fasterxml.jackson.dataformat.avro.deser;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import com.fasterxml.jackson.core.JsonToken;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;

public abstract class AvroScalarDecoder
{
    protected final static AvroScalarDecoder DECODER_BOOLEAN = new BooleanReader();
    protected final static AvroScalarDecoder DECODER_BYTES = new BytesReader();
    protected final static AvroScalarDecoder DECODER_DOUBLE = new DoubleReader();
    protected final static AvroScalarDecoder DECODER_FLOAT = new FloatReader();
    protected final static AvroScalarDecoder DECODER_INT = new IntReader();
    protected final static AvroScalarDecoder DECODER_LONG = new LongReader();
    protected final static AvroScalarDecoder DECODER_NULL = new NullReader();
    protected final static AvroScalarDecoder DECODER_STRING = new StringReader();

    public static AvroScalarDecoder createDecoder(Schema type)
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
            // First, a quick check: certain kinds of Unions can be "peeled off"...
            // This works if:
            // (a) one of types is NULL and
            // (b) the other type is scalar decoder
            {
                List<Schema> types = type.getTypes();
                if (types.size() == 2) {
                    if (types.get(0).getType() == Schema.Type.NULL) {
                        AvroScalarDecoder dec = createDecoder(types.get(1));
                        if (dec != null) {
                            return new NullableScalarDecoder(0, dec);
                        }
                    } else if (types.get(1).getType() == Schema.Type.NULL) {
                        AvroScalarDecoder dec = createDecoder(types.get(0));
                        if (dec != null) {
                            return new NullableScalarDecoder(1, dec);
                        }
                    }
                }
            }
            // otherwise, can't create simple decoder for unions
            return null;
            
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
    /* Simple leaf-value decoder implementations
    /**********************************************************************
     */

    protected final static class NullableScalarDecoder
        extends AvroScalarDecoder
    {
        protected final int _nullIndex;

        public final AvroScalarDecoder _nonNullDecoder;

        public NullableScalarDecoder(int nullIndex, AvroScalarDecoder nonNullDecoder)
        {
            _nullIndex = nullIndex;
            _nonNullDecoder = nonNullDecoder;            
        }
        
        @Override
        protected JsonToken readValue(AvroParserImpl parser, BinaryDecoder decoder)
            throws IOException
        {
            int index = decoder.readIndex();
            if (index == _nullIndex) {
                return JsonToken.VALUE_NULL;
            }
            return _nonNullDecoder.readValue(parser, decoder);
        }
    }
    
    protected final static class BooleanReader
        extends AvroScalarDecoder
    {
        @Override
        protected JsonToken readValue(AvroParserImpl parser, BinaryDecoder decoder)
            throws IOException {
            return decoder.readBoolean() ? JsonToken.VALUE_TRUE : JsonToken.VALUE_FALSE;
        }
    }
    
    protected final static class BytesReader
        extends AvroScalarDecoder
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
        extends AvroScalarDecoder
    {
        @Override public JsonToken readValue(AvroParserImpl parser, BinaryDecoder decoder)
            throws IOException
        {
            return parser.setNumber(decoder.readDouble());
        }
    }
    
    protected final static class FloatReader
        extends AvroScalarDecoder
    {
        @Override public JsonToken readValue(AvroParserImpl parser, BinaryDecoder decoder)
                throws IOException
        {
            return parser.setNumber(decoder.readFloat());
        }
    }
    
    protected final static class IntReader
        extends AvroScalarDecoder
    {
        @Override
        public JsonToken readValue(AvroParserImpl parser, BinaryDecoder decoder)
                throws IOException
        {
            return parser.setNumber(decoder.readInt());
        }
    }
    
    protected final static class LongReader
        extends AvroScalarDecoder
    {
        @Override
        public JsonToken readValue(AvroParserImpl parser, BinaryDecoder decoder)
            throws IOException
        {
            return parser.setNumber(decoder.readLong());
        }
    }
    
    protected final static class NullReader
        extends AvroScalarDecoder
    {
        @Override public JsonToken readValue(AvroParserImpl parser, BinaryDecoder decoder) {
            return JsonToken.VALUE_NULL;
        }
    }
    
    protected final static class StringReader
        extends AvroScalarDecoder
    {
        @Override
        public JsonToken readValue(AvroParserImpl parser, BinaryDecoder decoder)
            throws IOException
        {
            return parser.setString(decoder.readString());
        }
    }

    protected final static class EnumDecoder
        extends AvroScalarDecoder
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
        extends AvroScalarDecoder
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
