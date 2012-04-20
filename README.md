# Overview

This project contains [Jackson](http://http://wiki.fasterxml.com/JacksonHome) extension component for reading and writing data encoded using
[Apache Avro](http://avro.apache.org/) data format.
This project adds necessary abstractions on top to make things work with other Jackson functionality.

Project is licensed under [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0.txt).

# Status

Project is in its prototype phase. Stay tuned.

No Maven artifacts have been pushed; will do that if and once project gets bit more solid, independently tested.

## Maven dependency

To use this extension on Maven-based projects, use following dependency:

    <dependency>
      <groupId>com.fasterxml.jackson.dataformat</groupId>
      <artifactId>jackson-dataformat-avro</artifactId>
      <version>2.0.0</version>
    </dependency>

# Usage

## Simple usage

Usage is as with basic `JsonFactory`; most commonly you will just construct a standard `ObjectMapper` with `com.fasterxml.jackson.dataformat.avro.AvroFactory`, like so:

    ObjectMapper mapper = new ObjectMapper(new AvroFactory());
    User user = mapper.readValue(avroSource, User.class);

but you can also just use underlying `AvroFactory` and parser it produces, for event-based processing:

    AvroFactory factory = new AvroFactory();
    JsonParser parser = factory.createJsonParser(avroBytes); // don't be fooled by method name...
    while (parser.nextToken() != null) {
      // do something!
    }

# Documentation

* [Documentation](jackson-dataformat-avro/wiki/Documentation) IS TO BE WRITTEN
