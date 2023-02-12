package com.github.ankowals.example.kafka.data.builders;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;

public class EmailAddressRecordBuilder {

    private final Schema schema;
    private final GenericRecord record;

    public EmailAddressRecordBuilder() throws IOException {
        this.schema = new Schema.Parser().parse(getClass().getResourceAsStream("/subscriber.avro"))
                .getField("emailAddresses")
                .schema()
                .getElementType();

        this.record = new GenericData.Record(schema);
    }

    public static EmailAddressRecordBuilder builder() throws IOException {
        return new EmailAddressRecordBuilder();
    }

    public EmailAddressRecordBuilder email(String value) {
        this.record.put("email", value);
        return this;
    }

    public EmailAddressRecordBuilder address(boolean value) {
        this.record.put("address", value);
        return this;
    }

    public GenericRecord build() {
        return record;
    }
}
