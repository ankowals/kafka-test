package com.github.ankowals.example.kafka.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ankowals.example.kafka.EmailAddress;
import com.github.ankowals.example.kafka.Subscriber;
import com.github.ankowals.example.kafka.User;
import com.github.ankowals.example.kafka.framework.environment.kafka.SchemaReader;
import com.github.ankowals.example.kafka.data.GenericRecordJacksonMapper;
import com.github.ankowals.example.kafka.data.builders.SubscriberRecordBuilder;
import net.javacrumbs.jsonunit.core.Option;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static com.github.ankowals.example.kafka.data.GenericRecordFactory.emailAddress;
import static com.github.ankowals.example.kafka.data.GenericRecordMapper.toGenericRecord;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.json;
import static org.assertj.core.api.Assertions.assertThat;

public class GenericRecordTest {

    private static final SchemaReader schemaReader = new SchemaReader();

    @Test
    public void shouldConvertToGenericRecord() throws IOException {
        Schema schema = schemaReader.read("user.avro");

        User user = new User("Joe", 1, "red");

        GenericRecord record = toGenericRecord(user, schema);

        assertThatJson(record.toString()).and(
                a -> a.node("name").isEqualTo("Joe"),
                a -> a.node("favorite_number").isEqualTo(1),
                a -> a.node("favorite_color").isEqualTo("red"));

        User actual = new ObjectMapper().readValue(String.valueOf(record), User.class);

        assertThat(actual.getName()).isEqualTo("Joe");
        assertThat(actual.getFavorite_color()).isEqualTo("red");
        assertThat(actual.getFavorite_number()).isEqualTo(1);
    }

    @Test
    public void shouldConvertToGenericRecordUsingJacksonMapper() throws IOException {
        Schema schema = schemaReader.read("user.avro");

        User user = new User("Joe", 1, "red");

        GenericRecord record = GenericRecordJacksonMapper.toGenericRecord(user, schema);

        assertThatJson(record.toString())
                .isEqualTo("{ name: 'Joe', favorite_number: 1, favorite_color: 'red'}");
    }

    @Test
    public void shouldBuildSubscriberGenericRecord() throws Exception {
        SubscriberRecordBuilder builder = SubscriberRecordBuilder.builder();
        GenericRecord record = builder
                .age(17)
                .id(1)
                .fName("John")
                .lName("Doe")
                .phoneNumber("123456")
                .emailAddress(emailAddress("first@terefere.com"))
                .emailAddress(emailAddress("second@terefere.com"))
                .emailAddress(emailAddress("third@terefere.com"))
                .emailAddress(emailAddress("fourth@terefere.com"))
                .build();

        System.out.println(record.toString());

        assertThat(GenericData.get().validate(builder.getSchema(), record));

        assertThatJson(record.toString()).when(Option.IGNORING_ARRAY_ORDER).and(
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
    public void shouldMapToSubscriberGenericRecord() throws IOException {
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

        Schema schema = schemaReader.read("subscriber.avro");
        GenericRecord record = toGenericRecord(subscriber, schema);

        System.out.println(record.toString());

        assertThatJson(record.toString()).when(Option.IGNORING_ARRAY_ORDER).and(
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
