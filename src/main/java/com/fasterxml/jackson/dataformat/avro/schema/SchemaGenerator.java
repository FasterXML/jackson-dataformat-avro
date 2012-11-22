package com.fasterxml.jackson.dataformat.avro.schema;

import com.fasterxml.jackson.dataformat.avro.AvroSchema;

public class SchemaGenerator extends VisitorFormatWrapperImpl
{
    public SchemaGenerator() {
        super(new DefinedSchemas());
    }

    public AvroSchema getGeneratedSchema() {
        return new AvroSchema(getAvroSchema());
    }
}
