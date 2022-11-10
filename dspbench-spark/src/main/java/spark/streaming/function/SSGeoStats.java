package spark.streaming.function;

import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.streaming.GroupState;
import spark.streaming.model.CountryStats;
import spark.streaming.util.Configuration;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author luandopke
 */
public class SSGeoStats extends BaseFunction implements FlatMapGroupsWithStateFunction<String, Row, CountryStats, Row> {

    public SSGeoStats(Configuration config) {
        super(config);
    }

    @Override
    public Iterator<Row> call(String key, Iterator<Row> values, GroupState<CountryStats> state) throws Exception {
        List<Row> tuples = new ArrayList<>();
        CountryStats stats;
        String city;
        Row value;
        while (values.hasNext()) {
            super.calculateThroughput();

            value = values.next();
            city = value.getString(1);
            if (!state.exists()) {
                stats = new CountryStats(key);
            } else {
                stats = state.get();
            }
            stats.cityFound(city);
            state.update(stats);
            tuples.add(RowFactory.create(key, stats.getCountryTotal(), city, stats.getCityTotal(city), value.get(value.size() - 1)));
        }
        return tuples.iterator();
    }
}