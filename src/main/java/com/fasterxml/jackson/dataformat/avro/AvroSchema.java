package com.fasterxml.jackson.dataformat.avro;

import java.io.*;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.dataformat.avro.deser.AvroParserImpl;
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

    public BinaryDecoder decoder(InputStream in) {
         return DECODER_FACTORY.binaryDecoder(in, null);
    }

    public BinaryEncoder encoder(OutputStream out) {
        return ENCODER_FACTORY.binaryEncoder(out, null);
   }

    public AvroStructureReader getReader(InputStream in, AvroParserImpl parser)
    {
        AvroStructureReader r = _reader.get();
        if (r == null) {
            AvroReaderFactory f = new AvroReaderFactory();
            r = f.createReader(_avroSchema);
            _reader.set(r);
        }
        return r.newReader(parser, decoder(in));
    }
}
