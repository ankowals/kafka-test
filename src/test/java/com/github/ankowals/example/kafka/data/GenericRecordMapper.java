package com.github.ankowals.example.kafka.data;

import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class GenericRecordMapper {

    //using jackson mapper
    public static  <T> GenericRecord mapToGenericRecord(T object, Schema schema) throws IOException {
        final byte[] bytes = new AvroMapper().writer(new AvroSchema(schema)).writeValueAsBytes(object);
        GenericDatumReader<Object> genericRecordReader = new GenericDatumReader<>(schema);

        return (GenericRecord) genericRecordReader.read(null, DecoderFactory.get().binaryDecoder(bytes, null));
    }

    //when object properties have same names as in avro schema
    public static  <T> GenericRecord toGenericRecord(T object, Schema schema) throws IOException {
        ReflectDatumWriter<T> reflectDatumWriter = new ReflectDatumWriter<>(schema);
        GenericDatumReader<Object> genericRecordReader = new GenericDatumReader<>(schema);

        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
            reflectDatumWriter.write(object, EncoderFactory.get().directBinaryEncoder(bytes, null));
            return (GenericRecord) genericRecordReader.read(null, DecoderFactory.get().binaryDecoder(bytes.toByteArray(), null));
        }
    }
}
