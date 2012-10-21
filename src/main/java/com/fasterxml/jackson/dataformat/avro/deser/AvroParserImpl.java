package com.fasterxml.jackson.dataformat.avro.deser;

import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.dataformat.avro.AvroParser;
import com.fasterxml.jackson.dataformat.avro.AvroReadContext;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;

/**
 * Implementation class that exposes additional internal API
 * to be used as callbacks by {@link AvroReadContext} implementations.
 */
public class AvroParserImpl extends AvroParser
{
    protected final BinaryDecoder _decoder;
    
    public AvroParserImpl(IOContext ctxt, int parserFeatures, int avroFeatures,
            ObjectCodec codec, InputStream in)
    {
        super(ctxt, parserFeatures, avroFeatures, codec, in);
        _decoder = DecoderFactory.get().binaryDecoder(in, null);
    }

    public AvroParserImpl(IOContext ctxt, int parserFeatures, int avroFeatures,
            ObjectCodec codec,
            byte[] data, int offset, int len)
    {
        super(ctxt, parserFeatures, avroFeatures, codec,
                data, offset, len);
        _decoder = DecoderFactory.get().binaryDecoder(data, offset, len, null);
    }

    /*
    /**********************************************************
    /* Abstract method impls
    /**********************************************************
     */

    @Override
    protected void _initSchema(AvroSchema schema)
    {
        try {
            _avroContext = new RootContext(this, _decoder, schema.getAvroSchema());
        } catch (IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
    
    /*
    /**********************************************************
    /* Methods for AvroReadContext implementations
    /**********************************************************
     */

    protected void setAvroContext(AvroReadContext ctxt) {
        _avroContext = ctxt;
    }

}
