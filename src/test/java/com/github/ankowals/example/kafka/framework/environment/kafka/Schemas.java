package com.github.ankowals.example.kafka.framework.environment.kafka;

import com.github.ankowals.example.kafka.framework.ResourceLoader;
import org.apache.avro.Schema;

import java.io.IOException;

public class Schemas {

    private final Schema.Parser parser = new Schema.Parser();

    public Schema load(String path) throws IOException {
        return this.parser.parse(ResourceLoader.load(path));
    }
}
