package org.dspbench.applications.logprocessing;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import org.dspbench.bolt.AbstractBolt;
import org.dspbench.constants.BaseConstants;
import org.dspbench.applications.clickanalytics.ClickAnalyticsConstants;
import org.dspbench.util.geoip.IPLocation;
import org.dspbench.util.geoip.Location;
import org.dspbench.util.geoip.IPLocationFactory;

/**
 * User: domenicosolazzo
 */
public class GeographyBolt extends AbstractBolt {
    private IPLocation resolver;

    @Override
    public void initialize() {
        String ipResolver = config.getString(BaseConstants.BaseConf.GEOIP_INSTANCE);
        resolver = IPLocationFactory.create(ipResolver, config);
    }

    @Override
    public void execute(Tuple input) {
        incReceived();
        String ip = input.getStringByField(ClickAnalyticsConstants.Field.IP);
        
        Location location = resolver.resolve(ip);
        
        if (location != null) {
            String city = location.getCity();
            String country = location.getCountryName();
            incEmitted();
            collector.emit(input, new Values(country, city));
        }
        
        collector.ack(input);
        super.calculateThroughput();
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(ClickAnalyticsConstants.Field.COUNTRY, ClickAnalyticsConstants.Field.CITY);
    }
}
