package com.fasterxml.jackson.dataformat.avro;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;

/**
 * Convenience {@link AvroMapper}, which is mostly similar to simply
 * constructing a mapper with {@link AvroFactory}, but also adds little
 * bit of convenience around {@link AvroSchema} generation.
 * 
 * @since 2.4
 */
public class AvroMapper extends ObjectMapper
{
    private static final long serialVersionUID = 1L;

    public AvroMapper() {
        this(new AvroFactory());
    }

    public AvroMapper(AvroFactory f) {
        super(f);
    }

    protected AvroMapper(ObjectMapper src) {
        super(src);
    }
    
    @Override
    public AvroMapper copy()
    {
        _checkInvalidCopy(AvroMapper.class);
        return new AvroMapper(this);
    }

    @Override
    public Version version() {
        return PackageVersion.VERSION;
    }

    /**
     * @since 2.5
     */
    public AvroSchema schemaFor(Class<?> type) throws JsonMappingException
    {
    	AvroSchemaGenerator gen = new AvroSchemaGenerator();
    	this.acceptJsonFormatVisitor(type, gen);
    	return gen.getGeneratedSchema();
    }

    /**
     * @since 2.5
     */
    public AvroSchema schemaFor(JavaType type) throws JsonMappingException
    {
    	AvroSchemaGenerator gen = new AvroSchemaGenerator();
    	this.acceptJsonFormatVisitor(type, gen);
    	return gen.getGeneratedSchema();
    }
}
