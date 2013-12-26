package com.fasterxml.jackson.dataformat.avro;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Convenience {@link AvroMapper}, which is mostly similar to simply
 * constructing a mapper with {@link AvroFactory}.
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

    protected AvroMapper(ObjectMapper src)
    {
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
}
