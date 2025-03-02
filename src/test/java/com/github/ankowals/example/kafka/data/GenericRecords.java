package com.github.ankowals.example.kafka.data;

import com.github.ankowals.example.kafka.data.builders.EmailAddressRecordBuilder;
import java.io.IOException;
import org.apache.avro.generic.GenericRecord;

public class GenericRecords {

  public static GenericRecord email(String email) throws IOException {
    return EmailAddressRecordBuilder.builder().email(email).address(true).build();
  }
}
