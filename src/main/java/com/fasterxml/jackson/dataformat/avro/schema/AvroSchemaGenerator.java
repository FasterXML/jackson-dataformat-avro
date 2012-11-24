package com.fasterxml.jackson.dataformat.avro.schema;

import com.fasterxml.jackson.dataformat.avro.AvroSchema;

public class AvroSchemaGenerator extends VisitorFormatWrapperImpl
{
    public AvroSchemaGenerator() {
        super(new DefinedSchemas());
    }

    public AvroSchema getGeneratedSchema() {
        return new AvroSchema(getAvroSchema());
    }
}
