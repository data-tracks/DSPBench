package org.dspbench.applications.spamfilter;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import java.util.List;

import org.dspbench.spout.parser.Parser;
import org.dspbench.util.stream.StreamValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class JsonEmailParser extends Parser {
    private static final Logger LOG = LoggerFactory.getLogger(JsonEmailParser.class);
    
    private final Gson gson = new Gson();

    @Override
    public List<StreamValues> parse(String str) {
        StreamValues values = null;
        
        try {
            Email email = gson.fromJson(str, Email.class);
            
            values = new StreamValues();
            values.add(email.id);
            values.add(email.message);
            values.setMessageId(email.id);
            
            if (email.isSpam != null) {
                values.add(email.isSpam);
            }
        } catch (JsonSyntaxException ex) {
            LOG.error("Error parsing JSON encoded email", ex);
        }
        
        return ImmutableList.of(values);
    }
    
    private static class Email {
        public String id;
        public String message;
        public Boolean isSpam = null;
    }
}
