package org.dspbench.applications.wordcount;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.util.ArrayList;
import java.util.List;
import org.dspbench.base.sink.ConsoleSink;
import org.dspbench.base.source.FileSource;
import org.dspbench.core.Schema;
import org.dspbench.core.Sink;
import org.dspbench.core.Stream;
import org.dspbench.topology.impl.DataTracksPlan;
import org.dspbench.topology.impl.DataTracksTask;
import org.dspbench.topology.impl.LocalOperatorAdapter;
import org.dspbench.topology.impl.LocalOperatorInstance;
import org.dspbench.topology.impl.LocalSourceAdapter;
import org.dspbench.utils.Configuration;

/**
 * @author mayconbordin
 */
public class WordCountTask extends DataTracksTask {

    private int splitterThreads;
    private int counterThreads;


    @Override
    public void setConfiguration( Configuration config ) {
        super.setConfiguration( config );

        splitterThreads = config.getInt( WordCountConstants.Config.SPLITTER_THREADS, 1 );
        counterThreads = config.getInt( WordCountConstants.Config.COUNTER_THREADS, 1 );
    }



    @Override
    public void initialize() {
        Stream sentences = builder.createStream( WordCountConstants.Streams.SENTENCES, new Schema( WordCountConstants.Field.TEXT ) );
        Stream words = builder.createStream( WordCountConstants.Streams.WORDS, new Schema().keys( WordCountConstants.Field.WORD ) );
        Stream counts = builder.createStream( WordCountConstants.Streams.COUNTS, new Schema().keys( WordCountConstants.Field.WORD ).fields( WordCountConstants.Field.COUNT ) );

        builder.setSource( WordCountConstants.Component.SOURCE, source, sourceThreads );
        builder.publish( WordCountConstants.Component.SOURCE, sentences );
        builder.setTupleRate( WordCountConstants.Component.SOURCE, sourceRate );

        DataTracksPlan plan = getPlan();
        plan.create();
    }


    @Override
    public DataTracksPlan getPlan() {
        List<LocalSourceAdapter> sources = new ArrayList<>();
        LocalSourceAdapter wrapper = new LocalSourceAdapter();
        FileSource source = new FileSource("localhost:3666/ws" );
        source.setName( "source" );
        source.setParallelism( 1 );
        source.setConfigPrefix( "wc" );
        wrapper.setConfiguration( this.config );
        wrapper.setComponent( source );
        sources.add( wrapper );


        List<LocalOperatorAdapter> destinations = new ArrayList<>();
        LocalOperatorAdapter adapter = new LocalOperatorAdapter();
        adapter.setConfiguration( this.config );
        ConsoleSink destination = new ConsoleSink();
        adapter.setComponent( destination );
        adapter.setConfiguration( this.config );
        destination.setParallelism( 1 );
        destination.setConfigPrefix( "wc" );
        destination.setName( "destination" );
        destinations.add( adapter );

        return new DataTracksPlan(
                "wordcount",
                //"1-2{sql|SELECT COUNT(unwind) FROM UNWIND(SPLIT_REGEX($1, '\\s+')) GROUP BY unwind}-3",
                "0--1{sql|SELECT unwind, COUNT(*) FROM UNWIND(SELECT SPLIT($0, '\\\\s+') FROM $0) GROUP BY unwind}--2\\nIn\\nHttp{\\\"url\\\": \\\"localhost\\\", \\\"port\\\": \\\"3666\\\"}:0\\nOut\\nHttp{\\\"url\\\": \\\"localhost\\\", \\\"port\\\": \\\"4666\\\"}:2",
                sources,
                destinations );
    }


    @Override
    public String getConfigPrefix() {
        return WordCountConstants.PREFIX;
    }


}
