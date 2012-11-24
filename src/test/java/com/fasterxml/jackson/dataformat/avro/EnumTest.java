package com.fasterxml.jackson.dataformat.avro;

import com.fasterxml.jackson.databind.ObjectMapper;

public class EnumTest extends AvroTestBase
{
    protected final static String ENUM_SCHEMA_JSON = "{\n"
            +"\"type\": \"record\",\n"
            +"\"name\": \"Employee\",\n"
            +"\"fields\": [\n"
            +" {\"name\": \"gender\", \"type\": { \"type\" : \"enum\","
            +" \"name\": \"Gender\", \"symbols\": [\"M\",\"F\"] }"
            +"}\n"
            +"]}";

    protected enum Gender { M, F; } 
    
    protected static class Employee {
        public Gender gender;
    }
    
    public void testSimple() throws Exception
    {
        ObjectMapper mapper = new ObjectMapper(new AvroFactory());
        AvroSchema schema = parseSchema(ENUM_SCHEMA_JSON);
        Employee input = new Employee();
        input.gender = Gender.F;

        byte[] bytes = mapper.writer(schema).writeValueAsBytes(input);
        assertNotNull(bytes);
        assertEquals(1, bytes.length); // measured to be current exp size

        // and then back
        Employee output = mapper.reader(Employee.class).with(schema)
                .readValue(bytes);
        assertNotNull(output);
        assertEquals(Gender.F, output.gender);
    }
}
