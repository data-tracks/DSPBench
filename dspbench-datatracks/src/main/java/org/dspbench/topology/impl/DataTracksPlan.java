package org.dspbench.topology.impl;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import lombok.Value;
import org.dspbench.core.Sink;

@Value
public class DataTracksPlan {
    String name;
    String plan;
    List<LocalSourceAdapter> sources;
    List<LocalOperatorAdapter> destinations;


    public void create() {
        HttpClient client = HttpClient.newHttpClient();

        String json = String.format( "{\"name\": \"%s\", \"plan\": \"%s\"}", name, plan );

        HttpRequest request = HttpRequest.newBuilder()
                .uri( URI.create("http://localhost:2666/plans/create"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();
        try {
            client.send( request, HttpResponse.BodyHandlers.ofString() );
        } catch ( IOException | InterruptedException e ) {
            throw new RuntimeException( e );
        }

        start_plan(client);
    }


    private void start_plan( HttpClient client ) {
        String json = String.format( "{\"name\": \"%s\"}", name );

        HttpRequest request = HttpRequest.newBuilder()
                .uri( URI.create("http://localhost:2666/plans/start"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();
        try {
            client.send( request, HttpResponse.BodyHandlers.ofString() );
        } catch ( IOException | InterruptedException e ) {
            throw new RuntimeException( e );
        }
    }

}
