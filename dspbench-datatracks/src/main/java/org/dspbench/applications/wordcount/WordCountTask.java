package org.dspbench.applications.wordcount;

import java.util.List;
import org.dspbench.core.Schema;
import org.dspbench.core.Stream;
import org.dspbench.topology.impl.DataTracksPlan;
import org.dspbench.topology.impl.DataTracksTask;
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

    }


    @Override
    public DataTracksPlan getPlan() {
        return new DataTracksPlan( "wordcount",
                "1-2{sql|SELECT COUNT(unwind) FROM UNWIND(SPLIT_REGEX($1, '\\s+')) GROUP BY unwind}-3",
                List.of() );
    }


    @Override
    public String getConfigPrefix() {
        return WordCountConstants.PREFIX;
    }


}
