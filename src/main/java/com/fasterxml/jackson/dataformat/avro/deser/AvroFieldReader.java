package com.fasterxml.jackson.dataformat.avro.deser;

public class AvroFieldReader
{
    protected final AvroScalarReader _scalarReader;
    protected final AvroStructureReader _structureReader;

    public AvroFieldReader(AvroScalarReader scalarReader) {
        _scalarReader = scalarReader;
        _structureReader = null;
    }

    public AvroFieldReader(AvroStructureReader structureReader) {
        _structureReader = structureReader;
        _scalarReader = null;
    }
}
