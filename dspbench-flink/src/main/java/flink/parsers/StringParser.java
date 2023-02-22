package flink.parsers;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringParser extends Parser implements MapFunction<String, Tuple1<String>> {

    private static final Logger LOG = LoggerFactory.getLogger(StringParser.class);

    Configuration config;

    public StringParser(Configuration config){
        super.initialize(config);
        this.config = config;
    }

    @Override
    public Tuple1<String> map(String value) throws Exception {
        super.initialize(config);
        super.incBoth();
        if (StringUtils.isBlank(value))
            return null;

        return new Tuple1<String>(value);
    }

    @Override
    public Tuple1<?> parse(String input) {
        return null;
    }
}
