package com.github.ankowals.example.kafka.framework.environment.kafka;

import com.github.ankowals.example.kafka.framework.ResourceUtils;
import org.apache.avro.Schema;

import java.io.IOException;

public class SchemaReader {

    private final Schema.Parser parser = new Schema.Parser();

    public Schema read(String path) throws IOException {
        return parser.parse(ResourceUtils.getResourceContent(path));
    }
}
