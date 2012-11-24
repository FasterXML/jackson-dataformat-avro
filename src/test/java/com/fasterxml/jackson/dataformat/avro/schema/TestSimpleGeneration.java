package com.fasterxml.jackson.dataformat.avro.schema;

import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.*;

public class TestSimpleGeneration extends AvroTestBase
{
    public static class RootType
    {
        public String name;
        
        public int value;
        
        List<String> other;
    }

    @SuppressWarnings("serial")
    public static class StringMap extends HashMap<String,String> { }
    
    /*
    /**********************************************************
    /* Tests
    /**********************************************************
     */
    
    public void testBasic() throws Exception
    {
        ObjectMapper mapper = new ObjectMapper(new AvroFactory());
        AvroSchemaGenerator gen = new AvroSchemaGenerator();
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
        AvroSchemaGenerator gen = new AvroSchemaGenerator();
        mapper.acceptJsonFormatVisitor(Employee.class, gen);
        AvroSchema schema = gen.getGeneratedSchema();
        assertNotNull(schema);

        String json = schema.getAvroSchema().toString(true);        
        assertNotNull(json);
        AvroSchema s2 = parseSchema(json);
        assertNotNull(s2);

        Employee empl = new Employee();
        empl.name = "Bobbee";
        empl.age = 39;
        empl.emails = new String[] { "bob@aol.com", "bobby@gmail.com" };
        empl.boss = null;
        
        // So far so good: try producing actual Avro data...
        byte[] bytes = mapper.writer(schema).writeValueAsBytes(empl);
        assertNotNull(bytes);
        
        // and bring it back, too
        Employee e2 = getMapper().reader(Employee.class)
            .with(schema)
            .readValue(bytes);
        assertNotNull(e2);
        assertEquals(39, e2.age);
        assertEquals("Bobbee", e2.name);
    }

    public void testMap() throws Exception
    {
        ObjectMapper mapper = new ObjectMapper(new AvroFactory());
        AvroSchemaGenerator gen = new AvroSchemaGenerator();
        mapper.acceptJsonFormatVisitor(StringMap.class, gen);
        AvroSchema schema = gen.getGeneratedSchema();
        assertNotNull(schema);

        String json = schema.getAvroSchema().toString(true);
        assertNotNull(json);
        AvroSchema s2 = parseSchema(json);
        assertNotNull(s2);

        // should probably verify, maybe... ?
        
//        System.out.println("Map schema:\n"+json);
    }
}
