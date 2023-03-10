package com.github.ankowals.example.kafka.framework;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ResourceUtils {
    
    public static String getResourceFile(String path) throws IOException {
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        try (InputStream inputStream = classLoader.getResourceAsStream(path)) {
            if (inputStream == null)
                throw new NullPointerException("Resource file " + path + " was not found!");

            try(InputStreamReader inputStreamReader = new InputStreamReader(inputStream, UTF_8);
             BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
                return bufferedReader.lines().collect(Collectors.joining(System.lineSeparator()));
            }
        }
    }
}
