package org.dspbench.topology.impl;

import java.io.Serializable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.net.http.WebSocket.Listener;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import lombok.Getter;
import org.dspbench.core.Operator;
import org.dspbench.core.Task;
import org.dspbench.core.Tuple;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalOperatorInstance {
    private static final Logger LOG = LoggerFactory.getLogger(LocalOperatorInstance.class);
    @Getter
    private final Operator operator;
    @Getter
    private final int index;
    private final BlockingQueue<Tuple> buffer;

    public LocalOperatorInstance(Operator operator, int index) {
        this.operator = operator;
        this.index = index;

        buffer = new LinkedBlockingQueue<>();
    }


    public void processTuple(Tuple tuple) {
        try {
            buffer.put(tuple);
        } catch (InterruptedException ex) {
            LOG.error("Error in operator buffer", ex);
        }
    }


    @Getter
    private final Runnable processRunner = new Runnable() {
        public void run() {
            HttpClient client = HttpClient.newHttpClient();
            client.newWebSocketBuilder()
                    .buildAsync( URI.create( "ws://localhost:4666/ws" ), new Listener() {
                        @Override
                        public void onOpen( WebSocket webSocket ) {
                            System.out.println( "WebSocket connected." );
                            webSocket.request(1);
                        }


                        @Override
                        public CompletionStage<?> onText( WebSocket webSocket, CharSequence data, boolean last ) {
                            Map<String, Serializable> map = new HashMap<>();
                            map.put("data", data.toString());

                            Tuple tuple = new Tuple(0, 0, null, null,0,0, map );
                            buffer.add( tuple );
                            webSocket.request(1);
                            return null;
                        }


                        @Override
                        public CompletionStage<?> onBinary( WebSocket webSocket, ByteBuffer data, boolean last ) {
                            webSocket.request(1);
                            return Listener.super.onBinary( webSocket, data, last );
                        }


                        @Override
                        public void onError( WebSocket webSocket, Throwable error ) {
                            System.err.println( "WebSocket error: " + error.getMessage() );
                        }


                    } ).join();
            while (true) {
                try {
                    Tuple tuple = buffer.take();

                    synchronized (operator) {
                        operator.hooksBefore(tuple);
                        operator.process(tuple);
                        operator.hooksAfter(tuple);
                    }
                } catch (InterruptedException ex) {
                    LOG.error("Error in operator buffer", ex);
                } catch (Exception ex) {
                    LOG.error("Unknown error: " + ex.getMessage(), ex);
                }
            }
        }
    };
    
    @Getter
    private final Runnable timeRunner = new Runnable() {
        public void run() {
            synchronized (operator) {
                try {
                    operator.onTime();
                } catch (Exception ex) {
                    LOG.error("An exception ocurred in the onTime method", ex);
                }
            }
        }
    };
}