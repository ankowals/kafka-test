package com.github.ankowals.example.kafka.predicates;

import org.apache.avro.generic.GenericRecord;
import org.json.JSONObject;

import java.util.List;
import java.util.function.Predicate;

public class RecordPredicates {
    public static Predicate<List<Integer>> anyFound() {
        return list -> list.size() > 0;
    }

    public static Predicate<List<String>> sizeIs(int number) {
        return list -> list.size() == number;
    }

    public static Predicate<GenericRecord> nameEquals(String name) {
        return genericRecord -> new JSONObject(genericRecord.toString()).getString("name").equals(name);
    }

    public static Predicate<List<String>> containsAll(List<String> sublist) {
        return list-> list.containsAll(sublist);
    }
}
