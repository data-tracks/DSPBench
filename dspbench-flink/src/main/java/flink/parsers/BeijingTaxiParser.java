package flink.parsers;

import flink.util.Metrics;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class BeijingTaxiParser extends Metrics
        implements MapFunction<String, Tuple7<String, DateTime, Boolean, Integer, Integer, Double, Double>> {

    Configuration config;
    private static final Logger LOG = LoggerFactory.getLogger(BeijingTaxiParser.class);

    public BeijingTaxiParser(Configuration config) {
        super.initialize(config);
        this.config = config;
    }

    @Override
    public Tuple7<String, DateTime, Boolean, Integer, Integer, Double, Double> map(String value)
            throws Exception {
        
        super.initialize(config);
        super.incReceived();

        String[] temp = value.split(",");
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");

        if (temp.length != 7){
            LOG.info("NULL1");
            return new Tuple7<String, DateTime, Boolean, Integer, Integer, Double, Double>(null, null, true, 0, 0, 0.0, 0.0);//return null;
        }
        try {
            String carId = temp[0];
            DateTime date = formatter.parseDateTime(temp[2]);
            boolean occ = true;
            double lat = Double.parseDouble(temp[3]);
            double lon = Double.parseDouble(temp[4]);
            int speed = ((Double) Double.parseDouble(temp[5])).intValue();
            int bearing = Integer.parseInt(temp[6]);

            super.incEmitted();

            return new Tuple7<String, DateTime, Boolean, Integer, Integer, Double, Double>(carId, date, occ, speed, bearing, lat, lon);

        } catch (NumberFormatException ex) {
            System.out.println("Error parsing numeric value " + ex);
        } catch (IllegalArgumentException ex) {
            System.out.println("Error parsing date/time value   " + ex);
        }
        LOG.info("NULL2");
        return new Tuple7<String, DateTime, Boolean, Integer, Integer, Double, Double>(null, null, true, 0, 0, 0.0, 0.0);//return null;
    }

}
