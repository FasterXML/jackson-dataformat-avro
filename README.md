# Overview

This project contains [Jackson](http://http://wiki.fasterxml.com/JacksonHome) extension component for reading and writing data encoded using
[Apache Avro](http://avro.apache.org/) data format.

Project adds necessary abstractions on top to make things work with other Jackson functionality. It relies on standard Avro library for Avro Schema handling, and some parts of deserialization/serialization.

Project is licensed under [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0.txt).

# Status

Module is based on Jackson 2.x, and has been tested with simple Avro Schemas.
Both serialization and deserialization work.

## Maven dependency

To use this extension on Maven-based projects, use following dependency:

```xml
<dependency>
  <groupId>com.fasterxml.jackson.dataformat</groupId>
  <artifactId>jackson-dataformat-avro</artifactId>
  <version>2.4.3</version>
</dependency>
```

# Usage

## Schema Not Optional

Avro is strongly Schema-based, and all use requires an Avro Schema.
Since there is little metadata in encoded in Avro data, it is not possible to know anything about structure of data without Schema.

So the first step is to get an Avro Schema. Currently this means that you need to find JSON-based definitions of an Avro Schema, and use standard Avro library to read it in.
(note: in future we hope to simplify this process a bit).

One way to do this is:

```java
// note: AvroSchema is Jackson type that wraps "native" Avro Schema object:

String SCHEMA_JSON = ""{\n"
        +"\"type\": \"record\",\n"
        +"\"name\": \"Employee\",\n"
        +"\"fields\": [\n"
        +" {\"name\": \"name\", \"type\": \"string\"},\n"
        +" {\"name\": \"age\", \"type\": \"int\"},\n"
        +" {\"name\": \"emails\", \"type\": {\"type\": \"array\", \"items\": \"string\"}},\n"
        +" {\"name\": \"boss\", \"type\": [\"Employee\",\"null\"]}\n"
        +"]}";
Schema raw = new Schema.Parser().setValidate(true).parse(SCHEMA_JSON);
AvroSchema schema = new AvroSchema(raw);
```

However, note that there is another much more convenient way. If you are itching to know how,
peek at "Generating Avro Schema from POJO definition" section below; otherwise keep on reading.

## Creating ObjectMapper

(note: although you can use Streaming API -- if you really want -- it is unlikely to be very interesting to use directly)

Usage is as with basic `JsonFactory`; most commonly you will just construct a standard `ObjectMapper` with `com.fasterxml.jackson.dataformat.avro.AvroFactory`, like so:

```java
ObjectMapper mapper = new ObjectMapper(new AvroFactory());
```

## Reading Avro data

Assuming you have the `schema`, from above, and a POJO definition like:

```java
public class Employee
{
    public String name;
    public int age;
    public String[] emails;
    public Employee boss;
}
```

you can actually use data-binding like so:

```java
byte[] avroData = ... ; // or find an InputStream
Employee empl = mapper.reader(Employee.class)
   .with(schema)
   .readValue(avroData);
```

## Writing avro data

Writing Avro-encoded data follows similar pattern:

```java
byte[] avroData = mapper.writer(schema)
   .writeValueAsBytes(empl);
```

and that's about it, for now.

## Generating Avro Schema from POJO definition

Ok but wait -- you do not have to START with an Avro Schema. This module can
actually generate schema for you, starting with POJO definition(s)!
Here's how

```java
public class POJO {
  // your typical, Jackson-compatible POJO (with or without annotations)
}

ObjectMapper mapper = new ObjectMapper(new AvroFactory());
AvroSchemaGenerator gen = new AvroSchemaGenerator();
mapper.acceptJsonFormatVisitor(RootType.class, gen);
AvroSchema schemaWrapper = gen.getGeneratedSchema();

org.apache.avro.Schema avroSchema = schemaWrapper.getAvroSchema();
String asJson = avroSchema.toString(true);
```

So: you can generate native Avro Schema object very easily.

## Ok, so you REALLY want Streaming API

You can also just use underlying `AvroFactory` and parser it produces, for event-based processing:

```java
AvroFactory factory = new AvroFactory();
JsonParser parser = factory.createParser(avroBytes);
// but note: Schema is NOT optional, regardless:
parser.setSchema(schema);
while (parser.nextToken() != null) {
  // do something!
}
```

and similarly with `JsonGenerator`

# Issues

Currently, following things are likely to cause problems:

* More advanced features will probably not work well. This includes:
 * Polymorphic type handling
 * Object identity

# Documentation

* [Documentation](jackson-dataformat-avro/wiki/Documentation) IS TO BE WRITTEN
