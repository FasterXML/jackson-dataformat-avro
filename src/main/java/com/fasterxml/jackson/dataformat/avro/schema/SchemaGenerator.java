package com.fasterxml.jackson.dataformat.avro.schema;

public class SchemaGenerator extends VisitorFormatWrapperImpl
{
    public SchemaGenerator() {
        super(new DefinedSchemas());
    }
}
