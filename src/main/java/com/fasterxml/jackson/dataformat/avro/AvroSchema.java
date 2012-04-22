package com.fasterxml.jackson.dataformat.avro;

import com.fasterxml.jackson.core.FormatSchema;

import org.apache.avro.Schema;

public class AvroSchema implements FormatSchema
{
    public final static String TYPE_ID = "avro";
    
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
}
