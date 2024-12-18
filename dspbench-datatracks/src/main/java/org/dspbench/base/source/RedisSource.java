package org.dspbench.base.source;

import org.dspbench.base.source.parser.Parser;
import org.dspbench.core.Values;
import org.dspbench.base.constants.BaseConstants.BaseConfig;
import org.dspbench.utils.ClassLoaderUtils;
import org.dspbench.utils.Time;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

/**
 *
 * @author mayconbordin
 */
public class RedisSource extends BaseSource {
    private static final Logger LOG = LoggerFactory.getLogger(RedisSource.class);
    
    private LinkedBlockingQueue<String> queue;
    private Parser parser;
   
    @Override
    protected void initialize() {
        String parserClass = config.getString(getConfigKey(BaseConfig.SOURCE_PARSER));
        String host        = config.getString(getConfigKey(BaseConfig.REDIS_HOST));
        String pattern     = config.getString(getConfigKey(BaseConfig.REDIS_PATTERN));
        int port           = config.getInt(getConfigKey(BaseConfig.REDIS_PORT));
        int queueSize      = config.getInt(getConfigKey(BaseConfig.REDIS_QUEUE_SIZE));
        
        parser = (Parser) ClassLoaderUtils.newInstance(parserClass, "parser", LOG);
        parser.initialize(config);
        
        queue = new LinkedBlockingQueue<String>(queueSize);
        JedisPool pool = new JedisPool( new JedisPoolConfig(), host, port );

        ListenerThread listener = new ListenerThread( queue, pool, pattern );
        listener.start();
    }

    @Override
    public void nextTuple() {
        String message = queue.poll();
        
        if (message == null) {
            Time.sleep(50);
        } else {
            List<Values> tuples = parser.parse(message);
        
            if (tuples != null) {
                for (Values values : tuples) {
                    emit(values.getStreamId(), values);
                }
            }
        }
    }

    @Override
    public boolean hasNext() {
        return true;
    }
    
    private static class ListenerThread extends Thread {
        private final LinkedBlockingQueue<String> queue;
        private final JedisPool pool;
        private final String pattern;

        public ListenerThread(LinkedBlockingQueue<String> queue, JedisPool pool, String pattern) {
            this.queue = queue;
            this.pool = pool;
            this.pattern = pattern;
        }

        @Override
        public void run() {
            Jedis jedis = pool.getResource();
            
            try {
                jedis.psubscribe( new JedisListener( queue ), pattern);
            } finally {
                pool.returnResource(jedis);
            }
        }
    }
    
    private static class JedisListener extends JedisPubSub {
        private final LinkedBlockingQueue<String> queue;

        public JedisListener(LinkedBlockingQueue<String> queue) {
            this.queue = queue;
        }
        
        @Override
        public void onMessage(String channel, String message) {
            queue.offer(message);
        }

        @Override
        public void onPMessage(String pattern, String channel, String message) {
            queue.offer(message);
        }

        @Override
        public void onPSubscribe(String channel, int subscribedChannels) { }

        @Override
        public void onPUnsubscribe(String channel, int subscribedChannels) { }

        @Override
        public void onSubscribe(String channel, int subscribedChannels) { }

        @Override
        public void onUnsubscribe(String channel, int subscribedChannels) { }
    }
}
