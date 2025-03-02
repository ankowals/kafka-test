package com.github.ankowals.example.kafka.data.builders;

import com.github.ankowals.example.kafka.framework.environment.kafka.Schemas;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class EmailAddressRecordBuilder {

  private final GenericRecord genericRecord;

  public EmailAddressRecordBuilder() throws IOException {
    Schema schema =
        new Schemas().load("subscriber.avro").getField("emailAddresses").schema().getElementType();

    this.genericRecord = new GenericData.Record(schema);
  }

  public static EmailAddressRecordBuilder builder() throws IOException {
    return new EmailAddressRecordBuilder();
  }

  public EmailAddressRecordBuilder email(String value) {
    this.genericRecord.put("email", value);
    return this;
  }

  public EmailAddressRecordBuilder address(boolean value) {
    this.genericRecord.put("address", value);
    return this;
  }

  public GenericRecord build() {
    return this.genericRecord;
  }
}
