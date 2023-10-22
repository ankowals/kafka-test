package com.github.ankowals.example.kafka.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ankowals.example.kafka.data.GenericRecordMapper;
import com.github.ankowals.example.kafka.model.EmailAddress;
import com.github.ankowals.example.kafka.model.Subscriber;
import com.github.ankowals.example.kafka.model.User;
import com.github.ankowals.example.kafka.framework.environment.kafka.Schemas;
import com.github.ankowals.example.kafka.data.GenericRecordJacksonMapper;
import com.github.ankowals.example.kafka.data.builders.SubscriberRecordBuilder;
import net.javacrumbs.jsonunit.assertj.JsonAssertions;
import net.javacrumbs.jsonunit.core.Option;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static com.github.ankowals.example.kafka.data.GenericRecordFactory.email;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.json;

class GenericRecordTest {

    private static final Schemas SCHEMA_READER = new Schemas();

    @Test
    void shouldConvertToGenericRecord() throws IOException {
        Schema schema = SCHEMA_READER.load("user.avro");

        User user = new User("Joe", 1, "red");

        GenericRecord record = GenericRecordMapper.toGenericRecord(user, schema);

        JsonAssertions.assertThatJson(record.toString()).and(
                a -> a.node("name").isEqualTo("Joe"),
                a -> a.node("favorite_number").isEqualTo(1),
                a -> a.node("favorite_color").isEqualTo("red"));

        User actual = new ObjectMapper().readValue(String.valueOf(record), User.class);

        Assertions.assertThat(actual.getName()).isEqualTo("Joe");
        Assertions.assertThat(actual.getFavorite_color()).isEqualTo("red");
        Assertions.assertThat(actual.getFavorite_number()).isEqualTo(1);
    }

    @Test
    void shouldConvertToGenericRecordUsingJacksonMapper() throws IOException {
        Schema schema = SCHEMA_READER.load("user.avro");

        User user = new User("Joe", 1, "red");

        GenericRecord record = GenericRecordJacksonMapper.toGenericRecord(user, schema);

        JsonAssertions.assertThatJson(record.toString())
                .isEqualTo("{ name: 'Joe', favorite_number: 1, favorite_color: 'red'}");
    }

    @Test
    void shouldBuildSubscriberGenericRecord() throws Exception {
        SubscriberRecordBuilder builder = SubscriberRecordBuilder.builder();
        GenericRecord record = builder
                .age(17)
                .id(1)
                .fName("John")
                .lName("Doe")
                .phoneNumber("123456")
                .emailAddress(email("first@terefere.com"))
                .emailAddress(email("second@terefere.com"))
                .emailAddress(email("third@terefere.com"))
                .emailAddress(email("fourth@terefere.com"))
                .build();

        Assertions.assertThat(GenericData.get().validate(builder.getSchema(), record)).isTrue();
        JsonAssertions.assertThatJson(record.toString()).when(Option.IGNORING_ARRAY_ORDER).and(
                a -> a.node("id").isEqualTo(1),
                a -> a.node("fname").isEqualTo("John"),
                a -> a.node("lname").isEqualTo("Doe"),
                a -> a.node("phone_number").isString().isEqualTo("123456"),
                a -> a.node("age").isEqualTo(17),
                a -> a.node("emailAddresses").isArray().isEqualTo(json(
                        "[{email: 'first@terefere.com', address: true}, " +
                        "{email: 'second@terefere.com', address: true}, " +
                        "{email: 'third@terefere.com', address: true}, " +
                        "{email: 'fourth@terefere.com', address: true}]")));
    }

    @Test
    void shouldMapToSubscriberGenericRecord() throws IOException {
        Subscriber subscriber = Subscriber.builder()
                .age(17)
                .id(1)
                .fname("John")
                .lname("Doe")
                .phone_number("123456")
                .emailAddress(EmailAddress.builder().email("first@terefere.com").build())
                .emailAddress(EmailAddress.builder().email("second@terefere.com").build())
                .emailAddress(EmailAddress.builder().email("third@terefere.com").build())
                .emailAddress(EmailAddress.builder().email("fourth@terefere.com").build())
                .build();

        Schema schema = SCHEMA_READER.load("subscriber.avro");
        GenericRecord record = GenericRecordMapper.toGenericRecord(subscriber, schema);

        JsonAssertions.assertThatJson(record.toString()).when(Option.IGNORING_ARRAY_ORDER).and(
                a -> a.node("id").isEqualTo(1),
                a -> a.node("fname").isEqualTo("John"),
                a -> a.node("lname").isEqualTo("Doe"),
                a -> a.node("phone_number").isString().isEqualTo("123456"),
                a -> a.node("age").isEqualTo(17),
                a -> a.node("emailAddresses").isArray().isEqualTo(json(
                        "[{email: 'first@terefere.com', address: false}, " +
                                "{email: 'second@terefere.com', address: false}, " +
                                "{email: 'third@terefere.com', address: false}, " +
                                "{email: 'fourth@terefere.com', address: false}]")));
    }
}
