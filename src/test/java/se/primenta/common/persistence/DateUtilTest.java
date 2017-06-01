package se.primenta.common.persistence;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class DateUtilTest {

    @Test
    public void expectedListOfDates() {

        final int expected = 60;
        final Instant from = LocalDate.parse("2017-01-01").atStartOfDay().toInstant(ZoneOffset.UTC);
        final Instant to = LocalDate.parse("2017-03-01").atStartOfDay().toInstant(ZoneOffset.UTC);
        final List<String> list = DateUtil.isoDatesBetween(from, to);

        Assert.assertEquals(list.size(), expected);
        Assert.assertEquals(list.get(0), "2017-01-01");
        Assert.assertEquals(list.get(list.size() - 1), "2017-03-01");
    }

    @Test
    public void expectOneDateInList() {

        final Instant i = LocalDate.parse("2017-01-01").atStartOfDay().toInstant(ZoneOffset.UTC);
        final List<String> list = DateUtil.isoDatesBetween(i, i);
        Assert.assertEquals(list.size(), 1);
        Assert.assertEquals(list.get(0), "2017-01-01");
    }

    @Test
    public void isoDate() {

        final Instant i = LocalDate.parse("2017-06-06").atStartOfDay().toInstant(ZoneOffset.UTC);
        Assert.assertEquals(DateUtil.isoDate(i), "2017-06-06");
    }

    @Test
    public void isoDateFromNow() {

        final String date = LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC)
                .format(DateTimeFormatter.ISO_LOCAL_DATE);
        Assert.assertEquals(date, DateUtil.isoDate(Instant.now()));
    }

}