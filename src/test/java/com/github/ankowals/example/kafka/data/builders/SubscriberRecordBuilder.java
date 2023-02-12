package com.github.ankowals.example.kafka.data.builders;

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
        this.schema = new Schema.Parser().parse(getClass().getResourceAsStream("/subscriber.avro"));
        this.record = new GenericData.Record(schema);
        this.emailAddressesList = new ArrayList();
    }

    public static SubscriberRecordBuilder builder() throws IOException {
        return new SubscriberRecordBuilder();
    }

    public Schema getSchema() {
        return schema;
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
        this.record.put("emailAddresses", emailAddressesList);
        return record;
    }

        /*
    GenericData.Record avroRecord = new GenericData.Record(SchemaRegstryClient.getLatestSchema("test2"));
        System.out.println(avroRecord.getSchema().getFields());
        avroRecord.put("id", i);
        avroRecord.put("fName", "Sumit" + i);
        avroRecord.put("lName", "Gupta " +i);
        avroRecord.put("phone_number", "98911 " +i);
        avroRecord.put("age", i);
        avroRecord.put("emailAddresses.email", "sumit@gmail.com");
        avroRecord.put("emailAddresses.address", true);

        final GenericData.Record emailAddresses = new GenericData.Record(schema.getField("emailAddresses").schema());
emailAddresses.put("email", "sumit@gmail.com");
emailAddresses.put("address", true);
avroRecord.put("emailAddresses", emailAddresses);


 GenericRecord emailRecord = ((GenericRecord) record.get("emailAddresses")).get("email")


         GenericRecord friend1 = new GenericData.Record(childSchema);
        friend1.put("Name", "1");
        friend1.put("phoneNumber", "2");
        friend1.put("email", "3");
        friendList.add(friend1);
        record.put("friends", friendList);
     */
}
