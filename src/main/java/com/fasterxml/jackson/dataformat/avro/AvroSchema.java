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

    public static BinaryDecoder decoder(InputStream in, boolean buffering)
    {
        /*
        SoftReference<BinaryDecoder> ref = decoderRecycler.get();
        BinaryDecoder prev = (ref == null) ? null : ref.get();
        // Factory will check if the decoder has a matching type for reuse.
        // If not, it will drop the instance being reused and will return a new, proper one.
        BinaryDecoder next = buffering
            ? DECODER_FACTORY.binaryDecoder(in, prev)
            : DECODER_FACTORY.directBinaryDecoder(in, prev);

        decoderRecycler.set(new SoftReference<BinaryDecoder>(next));
        return next;
        */
        return buffering
                ? DECODER_FACTORY.binaryDecoder(in, null)
                : DECODER_FACTORY.directBinaryDecoder(in, null);
    }

    public static BinaryDecoder decoder(byte[] buffer, int offset, int len)
    {
        /*
        SoftReference<BinaryDecoder> ref = decoderRecycler.get();
        BinaryDecoder prev = (ref == null) ? null : ref.get();
        
        if (prev != null) {
            return DECODER_FACTORY.binaryDecoder(buffer, offset, len, prev);
        }
        prev = DECODER_FACTORY.binaryDecoder(buffer, offset, len, null);
        decoderRecycler.set(new SoftReference<BinaryDecoder>(prev));
        return prev;
        */
        return DECODER_FACTORY.binaryDecoder(buffer, offset, len, null);
    }

    public static BinaryEncoder encoder(OutputStream out, boolean buffering)
    {
        /*
        SoftReference<BinaryEncoder> ref = encoderRecycler.get();
        BinaryEncoder prev = (ref == null) ? null : ref.get();
        // Factory will check if the encoder has a matching type for reuse.
        // If not, it will drop the instance being reused and will return
        // a new, proper one.
        BinaryEncoder next =
            buffering
            ? ENCODER_FACTORY.binaryEncoder(out, prev)
            : ENCODER_FACTORY.directBinaryEncoder(out, prev);
        encoderRecycler.set(new SoftReference<BinaryEncoder>(next));

        return next;
        */
        return buffering
                ? ENCODER_FACTORY.binaryEncoder(out, null)
                : ENCODER_FACTORY.directBinaryEncoder(out, null);
    }

    /**
     * Method that will consider this schema instance (used as so-called "Writer Schema"),
     * and specified "Reader Schema" instance, and will either construct a new schema
     * with appropriate translations, to use for reading (if reader and writer schemas are
     * not same); or, if schemas are the same, return `this`.
     *<p>
     * Note that neither `this` instance nor `readerSchema` is ever modified: if an altered
     * version is needed, a new schema object will be constructed.
     *
     * @param readerSchema "Reader Schema" to use (in Avro terms): schema that specified how
     *    reader wants to see the data; specifies part of translation needed along with this
     *    schema (which would be "Writer Schema" in Avro terms).
     *
     * @since 2.9
     */
    public AvroSchema withReaderSchema(AvroSchema readerSchema) {
        Schema w = _avroSchema;
        Schema r = readerSchema.getAvroSchema();
        
        if (r.equals(w)) {
            return this;
        }
        Schema newSchema = Schema.applyAliases(w, r);
System.err.println("Translated schema ->\n"+newSchema.toString(true));
        return new AvroSchema(newSchema);
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

    @Override
    public String toString() {
        return String.format("{AvroSchema: name=%s}", _avroSchema.getFullName());
    }

    @Override
    public int hashCode() {
        return _avroSchema.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if ((o == null) || o.getClass() != getClass()) return false;
        AvroSchema other = (AvroSchema) o;
        return _avroSchema.equals(other._avroSchema);
    }
}
