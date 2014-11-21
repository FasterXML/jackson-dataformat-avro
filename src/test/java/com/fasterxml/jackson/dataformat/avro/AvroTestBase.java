package com.fasterxml.jackson.dataformat.avro;

import java.io.IOException;
import java.util.Arrays;

import junit.framework.TestCase;

import org.apache.avro.Schema;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.jackson.dataformat.avro.AvroSchema;

public abstract class AvroTestBase extends TestCase
{
    protected final String EMPLOYEE_SCHEMA_JSON = "{\n"
            +"\"type\": \"record\",\n"
            +"\"name\": \"Employee\",\n"
            +"\"fields\": [\n"
            +" {\"name\": \"name\", \"type\": \"string\"},\n"
            +" {\"name\": \"age\", \"type\": \"int\"},\n"
            +" {\"name\": \"emails\", \"type\": {\"type\": \"array\", \"items\": \"string\"}},\n"
            +" {\"name\": \"boss\", \"type\": [\"Employee\",\"null\"]}\n"
            +"]}";

    protected AvroSchema _employeeSchema;

    protected ObjectMapper _sharedMapper;
    
    protected static class Employee
    {
        public Employee() { }
        
        public String name;
        public int age;
        public String[] emails;
        public Employee boss;
    }
    
    protected AvroTestBase() { }

    protected AvroSchema getEmployeeSchema() {
        if (_employeeSchema == null) {
            _employeeSchema = parseSchema(EMPLOYEE_SCHEMA_JSON);
        }
        return _employeeSchema;
    }

    protected static AvroSchema parseSchema(String schemaJson) {
        return new AvroSchema(new Schema.Parser().setValidate(true).parse(schemaJson));        
    }

    protected ObjectMapper getMapper() {
        if (_sharedMapper == null) {
            _sharedMapper = new AvroMapper();
        }
        return _sharedMapper;
    }

    protected byte[] toAvro(Employee empl) throws IOException {
        return toAvro(empl, getMapper());
    }
    protected byte[] toAvro(Employee empl, ObjectMapper mapper) throws IOException {
        return mapper.writer(getEmployeeSchema()).writeValueAsBytes(empl);
    }

    protected void verifyException(Throwable e, String... matches)
    {
        String msg = e.getMessage();
        String lmsg = (msg == null) ? "" : msg.toLowerCase();
        for (String match : matches) {
            String lmatch = match.toLowerCase();
            if (lmsg.indexOf(lmatch) >= 0) {
                return;
            }
        }
        fail("Expected an exception with one of substrings ("+Arrays.asList(matches)+"): got one with message \""+msg+"\"");
    }

    protected static String aposToQuotes(String json) {
        return json.replace("'", "\"");
    }
}
