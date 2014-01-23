package com.fasterxml.jackson.dataformat.avro;

import org.junit.Assert;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;

public class SimpleGenerationTest extends AvroTestBase
{
    protected final String SCHEMA_WITH_BINARY_JSON = "{\n"
            +"\"type\": \"record\",\n"
            +"\"name\": \"Binary\",\n"
            +"\"fields\": [\n"
            +" {\"name\": \"name\", \"type\": \"string\"},\n"
            +" {\"name\": \"value\", \"type\": \"bytes\"}\n"
            +"]}";

    protected static class Binary {
        public String name;
        public byte[] value;

        public Binary() { }
        public Binary(String n, byte[] v) {
            name = n;
            value = v;
        }
    }
    
    public void testSimplest() throws Exception
    {
        Employee empl = new Employee();
        empl.name = "Bobbee";
        empl.age = 39;
        empl.emails = new String[] { "bob@aol.com", "bobby@gmail.com" };
        empl.boss = null;
        
        ObjectMapper mapper = new ObjectMapper(new AvroFactory());

        AvroSchema schema = getEmployeeSchema();
        byte[] bytes = mapper.writer(schema).writeValueAsBytes(empl);
        assertNotNull(bytes);

        // Currently we get this result... need to verify in future
        assertEquals(39, bytes.length);
        
        // read back actually
        Employee output = mapper.reader(schema).withType(Employee.class).readValue(bytes);
        assertNotNull(output);
        assertEquals(output.name, empl.name);
        assertEquals(output.age, empl.age);
    }

    public void testBinary() throws Exception
    {
        Binary bin = new Binary("Foo", new byte[] { 1, 2, 3, 4 });
        
        ObjectMapper mapper = new ObjectMapper(new AvroFactory());

        AvroSchema schema = parseSchema(SCHEMA_WITH_BINARY_JSON);
        byte[] bytes = mapper.writer(schema).writeValueAsBytes(bin);
        assertEquals(9, bytes.length);
        assertNotNull(bytes);
        Binary output = mapper.reader(schema).withType(Binary.class).readValue(bytes);
        assertNotNull(output);
        assertEquals("Foo", output.name);
        assertNotNull(output.value);
        Assert.assertArrayEquals(bin.value, output.value);
    }
}
