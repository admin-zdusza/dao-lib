package pl.zdusza;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

public class ClassPathFileResolver {

    public final String textFile(final String path) {
        InputStream inputStream = ClassPathFileResolver.class.getResourceAsStream(path);
        if (inputStream == null) {
            throw new RuntimeException("Missing file on project classpath: " + path);
        }
        try (BufferedReader file = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"))) {
            return file.lines().collect(Collectors.joining("\n"));
        } catch (IOException e) {
            throw new RuntimeException("Unable to read file: " + path);
        }
    }

    public final JsonObject jsonObjectFile(final String path) {
        return new JsonObject(this.textFile(path));
    }

    public final JsonArray jsonArrayFile(final String path) {
        return new JsonArray(this.textFile(path));
    }
}
