package com.fasterxml.jackson.dataformat.avro;

import java.io.*;

import com.fasterxml.jackson.core.FormatSchema;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

public class AvroSchema implements FormatSchema
{
    public final static String TYPE_ID = "avro";

    protected final static DecoderFactory DECODER_FACTORY = DecoderFactory.get();

    protected final static EncoderFactory ENCODER_FACTORY= EncoderFactory.get();
    
    protected final Schema _avroSchema;
    
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
}
