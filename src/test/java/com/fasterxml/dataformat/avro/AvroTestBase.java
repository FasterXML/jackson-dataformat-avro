package com.fasterxml.dataformat.avro;

import junit.framework.TestCase;

import org.apache.avro.Schema;

import com.fasterxml.jackson.dataformat.avro.AvroSchema;

public class AvroTestBase extends TestCase
{
    protected final String SIMPLE_SCHEMA = "{\n"
            +"\"type\": \"record\",\n"
            +"\"name\": \"Employee\",\n"
            +"\"fields\": [\n"
            +" {\"name\": \"name\", \"type\": \"string\"},\n"
            +" {\"name\": \"age\", \"type\": \"int\"},\n"
            +" {\"name\": \"emails\", \"type\": {\"type\": \"array\", \"items\": \"string\"}},\n"
            +" {\"name\": \"boss\", \"type\": [\"Employee\",\"null\"]}\n"
            +"]}";

    protected AvroSchema _simpleSchema;

    protected static class Employee
    {
        public String name;
        public int age;
        public String[] emails;
        public Employee boss;
    }
    
    protected AvroTestBase() {
        Schema s = new Schema.Parser().setValidate(true).parse(SIMPLE_SCHEMA);
        _simpleSchema = new AvroSchema(s);
    }
}
