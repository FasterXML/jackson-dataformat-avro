package com.fasterxml.jackson.dataformat.avro;

import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;

public class MapTest extends AvroTestBase
{
    protected final static String MAP_SCHEMA_JSON = "{\n"
            +"\"type\": \"record\",\n"
            +"\"name\": \"Container\",\n"
            +"\"fields\": [\n"
            +" {\"name\": \"stuff\", \"type\":{\n"
            +"    \"type\":\"map\", \"values\":[\"string\",\"null\"]"
            +" }}"
            +"]}"
            ;

    static class Container {
        public Map<String,String> stuff = new LinkedHashMap<String,String>();
    }
    
    public void testSimple() throws Exception
    {
        ObjectMapper mapper = new ObjectMapper(new AvroFactory());
        AvroSchema schema = parseSchema(MAP_SCHEMA_JSON);
        Container input = new Container();
        input.stuff.put("foo", "bar");
        input.stuff.put("a", "b");

        byte[] bytes = mapper.writer(schema).writeValueAsBytes(input);
        assertNotNull(bytes);

        assertEquals(1, bytes.length); // measured to be current exp size

        // and then back
        Container output = mapper.reader(Container.class).with(schema)
                .readValue(bytes);
        assertNotNull(output);
        assertNotNull(output.stuff);
        assertEquals(2, output.stuff.size());
        assertEquals("bar", output.stuff.get("foo"));
        assertEquals("a", output.stuff.get("b"));
    }
    
}
