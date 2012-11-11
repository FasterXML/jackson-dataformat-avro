package com.fasterxml.jackson.dataformat.avro;

import java.io.*;
import java.lang.ref.SoftReference;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.dataformat.avro.deser.AvroReaderFactory;
import com.fasterxml.jackson.dataformat.avro.deser.AvroStructureReader;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

/**
 * Wrapper for Schema information needed to encode and decode Avro-format
 * data.
 */
public class AvroSchema implements FormatSchema
{
    public final static String TYPE_ID = "avro";

    protected final static DecoderFactory DECODER_FACTORY = DecoderFactory.get();

    protected final static EncoderFactory ENCODER_FACTORY = EncoderFactory.get();

    protected final static ThreadLocal<SoftReference<BinaryDecoder>> decoderRecycler
        = new ThreadLocal<SoftReference<BinaryDecoder>>();
    
    protected final static ThreadLocal<SoftReference<BinaryEncoder>> encoderRecycler
        = new ThreadLocal<SoftReference<BinaryEncoder>>();
    
    protected final Schema _avroSchema;

    protected final AtomicReference<AvroStructureReader> _reader = new AtomicReference<AvroStructureReader>();
    
    public AvroSchema(Schema asch)
    {
        _avroSchema = asch;
    }

    @Override
    public String getSchemaType() {
        return TYPE_ID;
    }

    public Schema getAvroSchema() { return _avroSchema; }

    public static BinaryDecoder decoder(InputStream in)
    {
        SoftReference<BinaryDecoder> ref = decoderRecycler.get();
        BinaryDecoder prev = (ref == null) ? null : ref.get();
        
        if (prev != null) {
            return DECODER_FACTORY.binaryDecoder(in, prev);
        }
        prev = DECODER_FACTORY.binaryDecoder(in, null);
        decoderRecycler.set(new SoftReference<BinaryDecoder>(prev));
        return prev;
    }

    public static BinaryDecoder decoder(byte[] buffer, int offset, int len)
    {
        SoftReference<BinaryDecoder> ref = decoderRecycler.get();
        BinaryDecoder prev = (ref == null) ? null : ref.get();
        
        if (prev != null) {
            return DECODER_FACTORY.binaryDecoder(buffer, offset, len, prev);
        }
        prev = DECODER_FACTORY.binaryDecoder(buffer, offset, len, null);
        decoderRecycler.set(new SoftReference<BinaryDecoder>(prev));
        return prev;
    }
    
    
    public static BinaryEncoder encoder(OutputStream out)
    {
        SoftReference<BinaryEncoder> ref = encoderRecycler.get();
        BinaryEncoder prev = (ref == null) ? null : ref.get();
        
        if (prev != null) {
            return ENCODER_FACTORY.binaryEncoder(out, prev);
        }
        prev = ENCODER_FACTORY.binaryEncoder(out, null);
        encoderRecycler.set(new SoftReference<BinaryEncoder>(prev));
        return prev;
    }

    public AvroStructureReader getReader()
    {
        AvroStructureReader r = _reader.get();
        if (r == null) {
            AvroReaderFactory f = new AvroReaderFactory();
            r = f.createReader(_avroSchema);
            _reader.set(r);
        }
        return r;
    }
}
