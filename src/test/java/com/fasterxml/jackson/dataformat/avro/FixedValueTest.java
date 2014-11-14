package com.fasterxml.jackson.dataformat.avro;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;

import java.io.IOException;
import java.util.Random;

public class FixedValueTest extends AvroTestBase {
    private static final String FIXED_SCHEMA_JSON = "{\n"
            + "    \"type\": \"record\",\n"
            + "    \"name\": \"WithFixedField\",\n"
            + "    \"fields\": [\n"
            + "        {\"name\": \"fixedField\", \"type\": {\"name\": \"FixedFieldBytes\", \"type\": \"fixed\", \"size\": 4}}\n"
            + "    ]\n"
            + "}";

    public void testFixedField() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new AvroFactory());
        AvroSchema schema = parseSchema(FIXED_SCHEMA_JSON);

        WithFixedField in = new WithFixedField();
        byte[] bytes = {0, 1, 2, (byte) new Random().nextInt(256)};
        in.setValue(bytes);
        byte[] serialized = mapper.writer(schema).writeValueAsBytes(in);
        WithFixedField deser = mapper.reader(WithFixedField.class).with(schema).readValue(serialized);
        Assert.assertArrayEquals(bytes, deser.fixedField);
    }

    static class WithFixedField {
        public byte[] fixedField;

        void setValue(byte[] bytes) {
            this.fixedField = bytes;
        }
    }
}
