package cs455.hadoop.location;

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

  private final Text KEY = new Text();

  private final Text OUTPUT = new Text();

  private final StringBuilder sb = new StringBuilder();

  /**
   * Expected Output:
   * 
   * < song_id , hotness danceability duration fade_in energy key
   * loudness mode fade_out tempo time_signature e energy >
   * 
   */
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    ArrayList<String> itr = DocumentUtilities.splitString( value.toString() );

    String id = itr.get( 1 );

    if ( !id.isEmpty() && !id.equals( "song_id" ) )
    {
      KEY.set( id );

      sb.append( DocumentUtilities.parseDouble( itr.get( 2 ) ) ).append( "\t" ); // hotness
      sb.append( DocumentUtilities.parseDouble( itr.get( 5 ) ) ).append( "\t" ); // duration
      sb.append( DocumentUtilities.parseDouble( itr.get( 6 ) ) ).append( "\t" ); // fade_in
      sb.append( DocumentUtilities.parseDouble( itr.get( 8 ) ) ).append( "\t" ); // key
      sb.append( DocumentUtilities.parseDouble( itr.get( 10 ) ) )
          .append( "\t" ); // loudness
      sb.append( DocumentUtilities.parseDouble( itr.get( 11 ) ) )
          .append( "\t" ); // mode
      sb.append( DocumentUtilities.parseDouble( itr.get( 13 ) ) )
          .append( "\t" ); // fade_out
      sb.append( DocumentUtilities.parseDouble( itr.get( 14 ) ) )
          .append( "\t" ); // tempo
      sb.append( DocumentUtilities.parseDouble( itr.get( 15 ) ) ); // time_signature

      OUTPUT.set( sb.toString() );
      sb.setLength( 0 );
      context.write( KEY, OUTPUT );
    }
  }

}
