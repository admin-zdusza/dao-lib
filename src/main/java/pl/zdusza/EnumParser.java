package pl.zdusza;

import org.apache.commons.text.WordUtils;

import java.util.Arrays;
import java.util.Optional;

public final class EnumParser {

    private EnumParser() {
    }

    public static <T extends Enum<T>> Optional<T> parse(final String value, final Class<T> clazz) {
        try {
            return Optional.of(Enum.valueOf(clazz, value.replace("-", "_").toUpperCase()));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    public static <T extends Enum<T>> String format(final T[] values) {
        return Arrays.toString(Arrays
                .stream(values)
                .map(v -> v.name().replace("_", "-").toLowerCase())
                .toArray());
    }

    public static <T extends Enum<T>> String capitalized(final T value) {
        final String capitalized = WordUtils.capitalizeFully(value.name().replaceAll("_", " "));
        return (capitalized.substring(0, 1).toLowerCase()
                + capitalized.substring(1)).replaceAll(" ", "");
    }
}
