package com.github.ankowals.example.kafka.data.builders;

import com.github.ankowals.example.kafka.framework.environment.kafka.SchemaLoader;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SubscriberRecordBuilder {

    private final Schema schema;
    private final GenericRecord record;
    private final List<GenericRecord> emailAddressesList;

    public SubscriberRecordBuilder() throws IOException {
        this.schema = new SchemaLoader().load("subscriber.avro");
        this.record = new GenericData.Record(this.schema);
        this.emailAddressesList = new ArrayList();
    }

    public static SubscriberRecordBuilder builder() throws IOException {
        return new SubscriberRecordBuilder();
    }

    public Schema getSchema() {
        return this.schema;
    }

    public SubscriberRecordBuilder id(int value) {
        this.record.put("id", value);
        return this;
    }

    public SubscriberRecordBuilder fName(String value) {
        this.record.put("fname", value);
        return this;
    }

    public SubscriberRecordBuilder lName(String value) {
        this.record.put("lname", value);
        return this;
    }

    public SubscriberRecordBuilder phoneNumber(String value) {
        this.record.put("phone_number", value);
        return this;
    }

    public SubscriberRecordBuilder age(int value) {
        this.record.put("age", value);
        return this;
    }

    public SubscriberRecordBuilder emailAddress(GenericRecord emailAddress) {
        this.emailAddressesList.add(emailAddress);
        return this;
    }

    public GenericRecord build() {
        this.record.put("emailAddresses", this.emailAddressesList);
        return this.record;
    }
}
