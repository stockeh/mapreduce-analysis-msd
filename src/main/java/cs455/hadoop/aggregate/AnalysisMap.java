package cs455.hadoop.aggregate;

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
   * < song_id , hotness danceability duration fade_in energy key
   * loudness mode fade_out tempo time_signature e energy >
   * 
   */
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    ArrayList<String> itr = DocumentUtilities.splitString( value.toString() );

    String id = itr.get( 1 );
    String hotness = itr.get( 2 );

    double hot;
    try
    {
      hot = Double.parseDouble( hotness );
    } catch ( NullPointerException | NumberFormatException e )
    {
      hot = 0;
    }

    if ( !id.isEmpty() && hot != 0 )
    {
      sb.append( hotness ); // hotness
      sb.append( "\t" );
      // sb.append( itr.get( 4 ) ); // danceability
      // sb.append( "\t" );
      sb.append( itr.get( 5 ) ); // duration
      sb.append( "\t" );
      sb.append( itr.get( 6 ) ); // fade_in
      sb.append( "\t" );
      // sb.append( itr.get( 7 ) ); // energy
      // sb.append( "\t" );
      sb.append( itr.get( 8 ) ); // key
      sb.append( "\t" );
      sb.append( itr.get( 10 ) ); // loudness
      sb.append( "\t" );
      sb.append( itr.get( 11 ) ); // mode
      sb.append( "\t" );
      sb.append( itr.get( 13 ) ); // fade_out
      sb.append( "\t" );
      sb.append( itr.get( 14 ) ); // tempo
      sb.append( "\t" );
      sb.append( itr.get( 15 ) ); // time_signature
      sb.append( " " );

      songID.set( id );
      outputValue.set( sb.toString() );
      sb.setLength( 0 );

      context.write( songID, outputValue );
    }
  }

}
