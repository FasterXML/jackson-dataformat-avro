package com.fasterxml.jackson.dataformat.avro.schema;

import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.*;

public class TestSimple extends AvroTestBase
{
    public static class RootType
    {
        public String name;
        
        public int value;
        
        List<String> other;
    }
    
    /*
    /**********************************************************
    /* Tests
    /**********************************************************
     */
    
    public void testBasic() throws Exception
    {
        ObjectMapper mapper = new ObjectMapper(new AvroFactory());
        SchemaGenerator gen = new SchemaGenerator();
        mapper.acceptJsonFormatVisitor(RootType.class, gen);
        AvroSchema schema = gen.getGeneratedSchema();
        assertNotNull(schema);

        String json = schema.getAvroSchema().toString(true);
        assertNotNull(json);

        // And read it back too just for fun
        AvroSchema s2 = parseSchema(json);
        assertNotNull(s2);
        
//        System.out.println("Basic schema:\n"+json);
    }

    public void testEmployee() throws Exception
    {
        ObjectMapper mapper = new ObjectMapper(new AvroFactory());
        SchemaGenerator gen = new SchemaGenerator();
        mapper.acceptJsonFormatVisitor(Employee.class, gen);
        AvroSchema schema = gen.getGeneratedSchema();
        assertNotNull(schema);

        String json = schema.getAvroSchema().toString(true);
        assertNotNull(json);
        AvroSchema s2 = parseSchema(json);
        assertNotNull(s2);

//        System.out.println("Employee schema:\n"+json);
    }
}
