package com.fasterxml.jackson.dataformat.avro.schema;

import java.util.*;

import org.apache.avro.Schema;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonStringFormatVisitor;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonValueFormat;

public class StringVisitor extends VisitorBase
    implements JsonStringFormatVisitor
{
    protected JavaType _type;
    protected Set<String> _enums;

    public StringVisitor(JavaType t) {
        _type = t;
    }
    
    @Override
    public void format(JsonValueFormat format) { }

    @Override
    public void enumTypes(Set<String> enums) {
        _enums = enums;
    }

    @Override
    public Schema getAvroSchema() {
        if (_enums == null) {
            return Schema.create(Schema.Type.STRING);
        }
        return Schema.createEnum(getName(_type), "", getNamespace(_type),
                new ArrayList<String>(_enums));
    }
}
