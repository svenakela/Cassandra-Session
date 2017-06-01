package se.primenta.common.persistence;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility class for dates.
 *
 * @author Sven Wesley
 *
 */
public final class DateUtil {

    private static final DateTimeFormatter FORMAT = DateTimeFormatter.ISO_LOCAL_DATE;

    private DateUtil() {
    }

    /**
     * Creates a List of ISO Local Date formatted date strings between the two given dates.
     *
     * @param from
     *            date (inclusive)
     * @param to
     *            date (inclusive)
     * @return List of date strings
     */
    public static List<String> isoDatesBetween(final Instant from, final Instant to) {

        final LocalDateTime startDate = LocalDateTime.ofInstant(from, ZoneOffset.UTC);

        return Stream.iterate(startDate, d -> d.plusDays(1)).limit(ChronoUnit.DAYS.between(from, to) + 1)
                .map(date -> date.format(FORMAT)).collect(Collectors.toList());
    }

    /**
     * Create a ISO LOCAL DATE String from an Instant
     *
     * @param instant
     * @return String representing the date in a ISO LOCAL format.
     */
    public static String isoDate(final Instant instant) {
        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC).format(FORMAT);
    }
}
