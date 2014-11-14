package com.fasterxml.jackson.dataformat.avro;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * User: josh
 * Date: 11/13/14
 * Time: 10:13 PM
 */
public class FixedValueTest extends AvroTestBase {
  private static final String FIXED_SCHEMA_JSON = "{\n"
          + "    \"type\": \"record\",\n"
          + "    \"name\": \"WithFixedField\",\n"
          + "    \"fields\": [\n"
          + "        {\"name\": \"fixedField\", \"type\": {\"name\": \"FixedFieldBytes\", \"type\": \"fixed\", \"size\": 4}}\n"
          + "    ]\n"
          + "}";

  public void testFixedField() throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper(new AvroFactory());
    AvroSchema schema = parseSchema(FIXED_SCHEMA_JSON);

    mapper.writer(schema).writeValueAsBytes(new WithFixedField());
  }

  static class WithFixedField {
    public byte[] fixedField;

    WithFixedField() {
      this.fixedField = new byte[] {0, 1, 2, 3};
    }
  }
}
