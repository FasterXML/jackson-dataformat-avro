package com.fasterxml.jackson.dataformat.avro;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

public class SimpleMapperTest extends AvroTestBase
{
    // Test to verify that data format affects default state of order-props-alphabetically
    public void testDefaultSettings()
    {
        AvroFactory av = new AvroFactory();
        ObjectMapper mapper = new ObjectMapper(av);
        // should be defaulting to sort-alphabetically, due to Avro format requiring ordering
        assertTrue(mapper.isEnabled(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY));

        // and even with default mapper, may become so
        ObjectMapper vanilla = new ObjectMapper();
        assertFalse(vanilla.isEnabled(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY));
        ObjectReader r = vanilla.reader();
        assertFalse(r.isEnabled(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY));
        r = r.with(av);
        assertTrue(r.isEnabled(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY));
    }
}
