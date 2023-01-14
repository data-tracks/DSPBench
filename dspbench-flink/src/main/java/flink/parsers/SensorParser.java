package flink.parsers;

import com.google.common.collect.ImmutableMap;
import flink.constants.SpikeDetectionConstants;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Date;

public class SensorParser extends Parser implements MapFunction<String, Tuple4<String, Date, Double, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(SensorParser.class);

    private final String valueField;
    private final int valueFieldKey;

    Configuration config;

    private static final ImmutableMap<String, Integer> fieldList = ImmutableMap.<String, Integer>builder()
            .put("temp", 4)
            .put("humid", 5)
            .put("light", 6)
            .put("volt", 7)
            .build();

    public SensorParser(Configuration config){
        super.initialize(config);
        this.config = config;
        valueField = config.getString(SpikeDetectionConstants.Conf.PARSER_VALUE_FIELD, "temp");
        valueFieldKey = fieldList.get(valueField);
    }

    @Override
    public Tuple4<String, Date, Double, String> map(String value) throws Exception {
        super.initialize(config);
        super.calculateThroughput();
        DateTimeFormatter formatterMillis = new DateTimeFormatterBuilder()
                .appendYear(4, 4).appendLiteral("-").appendMonthOfYear(2).appendLiteral("-")
                .appendDayOfMonth(2).appendLiteral(" ").appendHourOfDay(2).appendLiteral(":")
                .appendMinuteOfHour(2).appendLiteral(":").appendSecondOfMinute(2)
                .appendLiteral(".").appendFractionOfSecond(3, 6).toFormatter();

        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

        String[] temp = value.split("\\s+");

        if (temp.length != 8)
            return null;

        String dateStr = String.format("%s %s", temp[0], temp[1]);
        DateTime date = null;

        try {
            date = formatterMillis.parseDateTime(dateStr);
        } catch (IllegalArgumentException ex) {
            try {
                date = formatter.parseDateTime(dateStr);
            } catch (IllegalArgumentException ex2) {
                System.out.println("Error parsing record date/time field, input record: " + value + ", " + ex2);
                return null;
            }
        }

        try {
            return new Tuple4<>(
                    temp[3],
                    date.toDate(),
                    Double.parseDouble(temp[valueFieldKey]),
                    Instant.now().toEpochMilli() + ""
            );
        } catch (NumberFormatException ex) {
            System.out.println("Error parsing record numeric field, input record: " + value + ", " + ex);
        }

        return null;
    }

    @Override
    public Tuple1<?> parse(String input) {
        return null;
    }
}
