package com.fasterxml.jackson.dataformat.avro;

import java.io.ByteArrayOutputStream;
import java.util.*;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MapTest extends AvroTestBase
{
    protected final static String MAP_SCHEMA_JSON = "{\n"
            +"\"type\": \"record\",\n"
            +"\"name\": \"Container\",\n"
            +"\"fields\": [\n"
            +" {\"name\": \"stuff\", \"type\":{\n"
//            +"    \"type\":\"map\", \"values\":[\"string\",\"null\"]"
            +"    \"type\":\"map\", \"values\": \"string\" "
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

        /* Clumsy, but turns out that failures from convenience methods may
         * get masked due to auto-close. Hence this trickery.
         */

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        JsonGenerator gen = mapper.getFactory().createGenerator(out);
        mapper.writer(schema).writeValue(gen, input);
        gen.close();
        byte[] bytes = out.toByteArray();
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
