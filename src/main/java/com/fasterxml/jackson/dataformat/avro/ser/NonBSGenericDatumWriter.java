package com.fasterxml.jackson.dataformat.avro.ser;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericDatumWriter;

/**
 * Need to sub-class to prevent encoder from crapping on writing an optional
 * Enum value (see [dataformat-avro#12])
 * 
 * @since 2.5.0
 */
public class NonBSGenericDatumWriter<D>
	extends GenericDatumWriter<D>
{
	public NonBSGenericDatumWriter(Schema root) {
		super(root);
	}
	
	@Override
	public int resolveUnion(Schema union, Object datum) {
		// Alas, we need a work-around first...
		if (datum == null) {
			return union.getIndexNamed(Type.NULL.getName());
		}
		if (datum instanceof String) { // String or Enum
			List<Schema> schemas = union.getTypes();
			for (int i = 0, len = schemas.size(); i < len; ++i) {
				Schema s = schemas.get(i);
				switch (s.getType()) {
				case STRING:
				case ENUM:
					return i;
				default:
				}
			}
		}
		// otherwise just default to base impl, stupid as it is...
		return super.resolveUnion(union, datum);
	}
}
