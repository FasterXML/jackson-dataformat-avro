package com.fasterxml.jackson.dataformat.avro.deser;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.avro.io.BinaryDecoder;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.dataformat.avro.AvroParser;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;

/**
 * Implementation class that exposes additional internal API
 * to be used as callbacks by {@link AvroReadContext} implementations.
 */
public class AvroParserImpl extends AvroParser
{
    protected final static byte[] NO_BYTES = new byte[0];
    
    protected final BinaryDecoder _decoder;

    protected ByteBuffer _byteBuffer;
    
    public AvroParserImpl(IOContext ctxt, int parserFeatures, int avroFeatures,
            ObjectCodec codec, InputStream in)
    {
        super(ctxt, parserFeatures, avroFeatures, codec, in);
        _decoder = AvroSchema.decoder(in);
    }

    public AvroParserImpl(IOContext ctxt, int parserFeatures, int avroFeatures,
            ObjectCodec codec,
            byte[] data, int offset, int len)
    {
        super(ctxt, parserFeatures, avroFeatures, codec,
                data, offset, len);
        _decoder = AvroSchema.decoder(data, offset, len);
    }

    /*
    /**********************************************************
    /* Abstract method impls
    /**********************************************************
     */

    @Override
    public JsonToken nextToken() throws IOException
    {
        _binaryValue = null;
        if (_closed) {
            return null;
        }
        JsonToken t = _avroContext.nextToken();
        _currToken = t;
        return t;
    }

    @Override
    public String nextFieldName() throws IOException
    {
        _binaryValue = null;
        if (_closed) {
            return null;
        }
        JsonToken t = _avroContext.nextToken();
        _currToken = t;
        return (t == JsonToken.FIELD_NAME) ? _avroContext.getCurrentName() : null;
    }

    @Override
    public boolean nextFieldName(SerializableString sstr) throws IOException
    {
        _binaryValue = null;
        if (_closed) {
            return false;
        }
        JsonToken t = _avroContext.nextToken();
        _currToken = t;
        if (t == JsonToken.FIELD_NAME) {
            return _avroContext.getCurrentName().equals(sstr.getValue());
        }
        return false;
    }

    @Override
    public String nextTextValue() throws IOException {
        return (nextToken() == JsonToken.VALUE_STRING) ? _textValue : null;
    }
    
    @Override
    protected void _initSchema(AvroSchema schema) {
        AvroStructureReader reader = schema.getReader();
        RootReader root = new RootReader();
        _avroContext = reader.newReader(root, this, _decoder);
    }
    
    /*
    /**********************************************************
    /* Methods for AvroReadContext implementations
    /**********************************************************
     */

    protected void setAvroContext(AvroReadContext ctxt) {
        if (ctxt == null) { // sanity check
            throw new IllegalArgumentException();
        }
        _avroContext = ctxt;
    }

    protected ByteBuffer borrowByteBuffer() {
        return _byteBuffer;
    }
    
    protected JsonToken setBytes(ByteBuffer bb)
    {
        int len = bb.remaining();
        if (len <= 0) {
            _binaryValue = NO_BYTES;
        } else {
            _binaryValue = new byte[len];
            bb.get(_binaryValue);
            // plus let's retain reference to this buffer, for reuse
            // (is safe due to way Avro impl handles them)
            _byteBuffer = bb;
        }
        return JsonToken.VALUE_EMBEDDED_OBJECT;
    }

    protected JsonToken setBytes(byte[] b)
    {
        _binaryValue = b;
        return JsonToken.VALUE_EMBEDDED_OBJECT;
    }
    
    protected JsonToken setNumber(int v) {
        _numberInt = v;
        _numTypesValid = NR_INT;
        return JsonToken.VALUE_NUMBER_INT;
    }

    protected JsonToken setNumber(long v) {
        _numberLong = v;
        _numTypesValid = NR_LONG;
        return JsonToken.VALUE_NUMBER_INT;
    }

    protected JsonToken setNumber(float v) {
        _numberDouble = v;
        _numTypesValid = NR_DOUBLE;
        return JsonToken.VALUE_NUMBER_FLOAT;
    }

    protected JsonToken setNumber(double v) {
        _numberDouble = v;
        _numTypesValid = NR_DOUBLE;
        return JsonToken.VALUE_NUMBER_FLOAT;
    }

    protected JsonToken setString(String str) {
        _textValue = str;
        return JsonToken.VALUE_STRING;
    }
}
