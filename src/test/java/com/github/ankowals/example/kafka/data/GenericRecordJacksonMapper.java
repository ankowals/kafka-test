package com.github.ankowals.example.kafka.data;

import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;

public class GenericRecordJacksonMapper {

    public static  <T> GenericRecord toGenericRecord(T object, Schema schema) throws IOException {
        final byte[] bytes = new AvroMapper().writer(new AvroSchema(schema)).writeValueAsBytes(object);
        GenericDatumReader<Object> genericRecordReader = new GenericDatumReader<>(schema);

        return (GenericRecord) genericRecordReader.read(null, DecoderFactory.get().binaryDecoder(bytes, null));
    }
}
