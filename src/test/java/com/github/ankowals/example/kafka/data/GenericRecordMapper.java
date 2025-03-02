package com.github.ankowals.example.kafka.data;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;

public class GenericRecordMapper {

  public static <T> GenericRecord toGenericRecord(T object, Schema schema) throws IOException {
    ReflectDatumWriter<T> reflectDatumWriter = new ReflectDatumWriter<>(schema);
    GenericDatumReader<Object> genericRecordReader = new GenericDatumReader<>(schema);

    try (ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
      reflectDatumWriter.write(object, EncoderFactory.get().directBinaryEncoder(bytes, null));
      return (GenericRecord)
          genericRecordReader.read(
              null, DecoderFactory.get().binaryDecoder(bytes.toByteArray(), null));
    }
  }
}
