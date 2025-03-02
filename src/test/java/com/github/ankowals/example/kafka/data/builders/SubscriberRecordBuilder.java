package com.github.ankowals.example.kafka.data.builders;

import com.github.ankowals.example.kafka.framework.environment.kafka.Schemas;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class SubscriberRecordBuilder {

  private final GenericRecord genericRecord;
  private final List<GenericRecord> emailAddressesList;

  public SubscriberRecordBuilder() throws IOException {
    Schema schema = new Schemas().load("subscriber.avro");
    this.genericRecord = new GenericData.Record(schema);
    this.emailAddressesList = new ArrayList<>();
  }

  public static SubscriberRecordBuilder builder() throws IOException {
    return new SubscriberRecordBuilder();
  }

  public SubscriberRecordBuilder id(int value) {
    this.genericRecord.put("id", value);
    return this;
  }

  public SubscriberRecordBuilder fName(String value) {
    this.genericRecord.put("fname", value);
    return this;
  }

  public SubscriberRecordBuilder lName(String value) {
    this.genericRecord.put("lname", value);
    return this;
  }

  public SubscriberRecordBuilder phoneNumber(String value) {
    this.genericRecord.put("phone_number", value);
    return this;
  }

  public SubscriberRecordBuilder age(int value) {
    this.genericRecord.put("age", value);
    return this;
  }

  public SubscriberRecordBuilder emailAddress(GenericRecord emailAddress) {
    this.emailAddressesList.add(emailAddress);
    return this;
  }

  public GenericRecord build() {
    this.genericRecord.put("emailAddresses", this.emailAddressesList);
    return this.genericRecord;
  }
}
