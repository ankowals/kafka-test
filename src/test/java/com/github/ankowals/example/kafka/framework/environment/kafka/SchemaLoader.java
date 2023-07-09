package com.github.ankowals.example.kafka.framework.environment.kafka;

import com.github.ankowals.example.kafka.framework.ResourceUtils;
import org.apache.avro.Schema;

import java.io.IOException;

public class SchemaLoader {

    private final Schema.Parser parser = new Schema.Parser();

    public Schema load(String path) throws IOException {
        return this.parser.parse(ResourceUtils.getResourceContent(path));
    }
}
