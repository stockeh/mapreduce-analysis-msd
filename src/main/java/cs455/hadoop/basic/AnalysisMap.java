package cs455.hadoop.basic;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import cs455.hadoop.util.DocumentUtilities;

/**
 * Mapper class for the analysis data files.
 * 
 * @author stock
 *
 */
public class AnalysisMap extends Mapper<LongWritable, Text, Text, Text> {

  private final Text songID = new Text();

  private final Text outputValue = new Text();

  private final StringBuilder sb = new StringBuilder();

  /**
   * Expected Output:
   * 
   * < song_id , loudness fade_duration hotness duration danceability
   * energy>
   * 
   */
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    ArrayList<String> itr = DocumentUtilities.splitString( value.toString() );

    String id = itr.get( 1 );

    if ( id.length() > 0 )
    {
      sb.append( itr.get( 10 ) ); // loudness
      sb.append( "\t" );
      sb.append( itr.get( 6 ) ); // fadeInDuration
      sb.append( "\t" );
      sb.append( itr.get( 13 ) ); // startFadeOutTime
      sb.append( "\t" );
      sb.append( itr.get( 2 ) ); // hotness
      sb.append( "\t" );
      sb.append( itr.get( 5 ) ); // duration
      sb.append( "\t" );
      sb.append( itr.get( 4 ) ); // danceability
      sb.append( "\t" );
      sb.append( itr.get( 7 ).trim() ); // energy
      sb.append( "0" );

      songID.set( id );
      outputValue.set( sb.toString() );
      sb.setLength( 0 );

      context.write( songID, outputValue );
    }
  }

}
