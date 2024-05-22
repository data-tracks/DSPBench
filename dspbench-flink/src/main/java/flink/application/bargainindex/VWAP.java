package flink.application.bargainindex;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.joda.time.DateTimeComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.constants.BargainIndexConstants;
import flink.util.Metrics;

public class VWAP extends Metrics implements FlatMapFunction<Tuple5<String, Double, Integer, Date, Integer>, Tuple4<String, Double, DateTime, DateTime>>{
    private static final DateTimeComparator dateOnlyComparator = DateTimeComparator.getDateOnlyInstance();
    
    private Map<String, Vwap> stocks;
    private String period;

    private static final Logger LOG = LoggerFactory.getLogger(VWAP.class);
    Configuration config;

    public VWAP(Configuration config){
        super.initialize(config);
        this.config = config;

        period = config.getString(BargainIndexConstants.Conf.VWAP_PERIOD, "daily");
        
        stocks = new HashMap<>();
    }

    @Override
    public void flatMap(Tuple5<String, Double, Integer, Date, Integer> value, Collector<Tuple4<String, Double, DateTime, DateTime>> out) {
        super.initialize(config);
        super.incReceived();
        
        String stock  = value.getField(0);
        double price  = (double) value.getField(1);
        int volume    = (int) value.getField(2);
        DateTime date = (DateTime) value.getField(3);
        int interval   = value.getField(4);

        Vwap vwap = stocks.get(stock);

        if (withinPeriod(vwap, date)) {
            vwap.update(volume, price, date.plusSeconds(interval));
            super.incEmitted();
            //collector.emit(input, new Values(stock, vwap.getVwap(), vwap.getStartDate(), vwap.getEndDate()));
            out.collect(new Tuple4<String,Double,DateTime,DateTime>(stock,  vwap.getVwap(), vwap.getStartDate(), vwap.getEndDate()));
        } else {
            if (vwap != null) {
                super.incEmitted();
                //collector.emit(new Values(stock, vwap.getVwap(), vwap.getStartDate(), vwap.getEndDate()));
                out.collect(new Tuple4<String,Double,DateTime,DateTime>(stock,  vwap.getVwap(), vwap.getStartDate(), vwap.getEndDate()));
            }
            
            vwap = new Vwap(volume, price, date, date.plusSeconds(interval));
            stocks.put(stock, vwap);
            
            super.incEmitted();
            out.collect(new Tuple4<String,Double,DateTime,DateTime>(stock,  vwap.getVwap(), vwap.getStartDate(), vwap.getEndDate()));
        }
        
    }

    private boolean withinPeriod(Vwap vwap, DateTime quoteDate) {
        if (vwap == null) return false;
        
        DateTime vwapDate  = vwap.getStartDate();
        
        switch (period) {
            case "minutely":
                return ((dateOnlyComparator.compare(vwapDate, quoteDate) == 0)
                        && vwapDate.getMinuteOfDay() == quoteDate.getMinuteOfDay());
            
            case "hourly":
                return ((dateOnlyComparator.compare(vwapDate, quoteDate) == 0)
                        && vwapDate.getHourOfDay() == quoteDate.getHourOfDay());
                
            case "daily":
                return (dateOnlyComparator.compare(vwapDate, quoteDate) == 0);
                
            case "weekly":
                return (vwapDate.getYear() == quoteDate.getYear() &&
                        vwapDate.getWeekOfWeekyear() == quoteDate.getWeekOfWeekyear());
                
            case "monthly":
                return (vwapDate.getYear() == quoteDate.getYear() &&
                        vwapDate.getMonthOfYear() == quoteDate.getMonthOfYear());
        }
        
        return false;
    }
    
    public static final class Vwap {
        private long totalShares = 0;
        private double tradedValue = 0;
        private double vwap = 0;
        private DateTime startDate;
        private DateTime endDate;

        public Vwap(long shares, double price, DateTime start, DateTime end) {
            this.startDate = start;
            this.endDate = end;
            
            update(shares, price, null);
        }
        
        public void update(long shares, double price, DateTime date) {
            totalShares += shares;
            tradedValue += (shares*price);
            vwap = tradedValue/totalShares;
            
            if (date != null) {
                endDate = date;
            }
        }

        public double getVwap() {
            return vwap;
        }

        public DateTime getStartDate() {
            return startDate;
        }

        public DateTime getEndDate() {
            return endDate;
        }
    }
}
