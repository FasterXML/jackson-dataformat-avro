package com.fasterxml.jackson.dataformat.avro.schema;

import com.fasterxml.jackson.dataformat.avro.AvroSchema;

public class AvroSchemaGenerator extends VisitorFormatWrapperImpl
{
    public AvroSchemaGenerator() {
        // NOTE: null is fine here, as provider links itself after construction
        super(new DefinedSchemas(), null);
    }

    public AvroSchema getGeneratedSchema() {
        return new AvroSchema(getAvroSchema());
    }
}
