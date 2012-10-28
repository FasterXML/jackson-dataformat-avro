package com.fasterxml.jackson.dataformat.avro.deser;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.dataformat.avro.AvroReadContext;

/**
 * Base class for handlers for Avro structured types (or, in case of
 * root values, wrapped scalar values).
 */
public abstract class AvroStructureReader
    extends AvroReadContext
{
    protected AvroStructureReader(AvroReadContext parent, int type) {
        super(parent);
        _type = type;
    }
    
    /*
    /**********************************************************************
    /* Reader API
    /**********************************************************************
     */

    /**
     * Method for creating actual instance to use for reading (initial
     * instance constructed is so-called blue print).
     */
    public abstract AvroStructureReader newReader(AvroParserImpl parser, BinaryDecoder decoder);

    @Override
    public abstract JsonToken nextToken() throws IOException;

    protected void throwIllegalState(int state) {
        throw new IllegalStateException("Illegal state for reader of type "
                +getClass().getName()+": "+state);
    }

    protected <T> T _throwUnsupported() {
        throw new IllegalStateException("Can not call on "+getClass().getName());
    }
    
    /*
    /**********************************************************************
    /* Factory methods
    /**********************************************************************
     */
    
    /**
     * Method for creating a reader instance for specified type.
     */
    public static AvroStructureReader createReader(Schema schema)
    {
        switch (schema.getType()) {
        case ARRAY:
            return createArrayReader(schema);
        case MAP: 
            return createMapReader(schema);
        case RECORD:
            return createRecordReader(schema);
        case UNION:
            return createUnionReader(schema);
        default:
            // for other types, we need wrappers
            return new ScalarReaderWrapper(AvroScalarReader.createDecoder(schema));
        }
    }

    private static AvroStructureReader createArrayReader(Schema schema)
    {
        Schema elementType = schema.getElementType();
        AvroScalarReader scalar = AvroScalarReader.createDecoder(elementType);
        if (scalar != null) {
            return ArrayReader.scalar(scalar);
        }
        return ArrayReader.nonScalar(createReader(elementType));
    }

    private static AvroStructureReader createMapReader(Schema schema)
    {
        Schema elementType = schema.getElementType();
        AvroScalarReader dec = AvroScalarReader.createDecoder(elementType);
        if (dec != null) {
            return new MapReader(dec);
        }
        return new MapReader(createReader(elementType));
    }

    private static AvroStructureReader createRecordReader(Schema schema)
    {
        final List<Schema.Field> fields = schema.getFields();
        AvroFieldWrapper[] fieldReaders = new AvroFieldWrapper[fields.size()];
        int i = 0;
        for (Schema.Field field : fields) {
            fieldReaders[i++] = createFieldReader(field);
        }
        return new RecordReader(fieldReaders);
    }
    
    private static AvroStructureReader createUnionReader(Schema schema)
    {
        final List<Schema> types = schema.getTypes();
        AvroStructureReader[] typeReaders = new AvroStructureReader[types.size()];
        int i = 0;
        for (Schema type : types) {
            typeReaders[i++] = createReader(type);
        }
        return new UnionReader(typeReaders);
    }

    private static AvroFieldWrapper createFieldReader(Schema.Field field) {
        return createFieldReader(field.name(), field.schema());
    }

    private static AvroFieldWrapper createFieldReader(String name, Schema type)
    {
        AvroScalarReader scalar = AvroScalarReader.createDecoder(type);
        if (scalar != null) {
            return new AvroFieldWrapper(name, scalar);
        }
        return new AvroFieldWrapper(name, createReader(type));
    }
}
