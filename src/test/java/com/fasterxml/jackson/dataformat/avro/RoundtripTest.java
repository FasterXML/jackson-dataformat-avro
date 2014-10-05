package com.fasterxml.jackson.dataformat.avro;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

public class RoundtripTest extends MapTest
{
    public void testIssue9() throws Exception
    {
        AvroSchema jsch = getEmployeeSchema();
        ObjectMapper mapper = new ObjectMapper(new AvroFactory());
        ObjectWriter writ = mapper.writer(jsch);
        ObjectMapper unzip = new ObjectMapper();
        byte[] avroData = writ.writeValueAsBytes(unzip.readTree
                ("{\"name\":\"Bob\",\"age\":15,\"emails\":[]}"));
        assertNotNull(avroData);
    }

}
