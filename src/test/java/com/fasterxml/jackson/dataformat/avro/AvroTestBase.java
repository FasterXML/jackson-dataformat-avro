package com.fasterxml.jackson.dataformat.avro;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.avro.Schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
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

    protected ObjectMapper MAPPER;
    
    protected static class Employee
    {
        public Employee() { }
        
        public String name;
        public int age;
        public String[] emails;
        public Employee boss;
    }
    
    protected AvroTestBase() {
    }

    protected AvroSchema getEmployeeSchema()
    {
        if (_employeeSchema == null) {
            _employeeSchema = parseSchema(EMPLOYEE_SCHEMA_JSON);
        }
        return _employeeSchema;
    }

    protected AvroSchema parseSchema(String schemaJson)
    {
        return new AvroSchema(new Schema.Parser().setValidate(true).parse(schemaJson));        
    }
    
    protected ObjectMapper getMapper() {
        if (MAPPER == null) {
            MAPPER = new ObjectMapper(new AvroFactory());
        }
        return MAPPER;
    }
    
    protected byte[] toAvro(Employee empl) throws IOException {
        return toAvro(empl, getMapper());
    }
    
    protected byte[] toAvro(Employee empl, ObjectMapper mapper) throws IOException
    {
        return mapper.writer(getEmployeeSchema()).writeValueAsBytes(empl);
    }
}
