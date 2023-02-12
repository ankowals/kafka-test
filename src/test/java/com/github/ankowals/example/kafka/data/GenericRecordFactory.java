package com.github.ankowals.example.kafka.data;

import com.github.ankowals.example.kafka.data.builders.EmailAddressRecordBuilder;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;

public class GenericRecordFactory {

    public static GenericRecord emailAddress(String email) throws IOException {
        return EmailAddressRecordBuilder.builder()
                .email(email)
                .address(true)
                .build();
    }
}
