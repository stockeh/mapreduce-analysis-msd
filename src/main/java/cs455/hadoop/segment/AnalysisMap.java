package cs455.hadoop.segment;

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

  private final Text ID = new Text();

  private final Text OUTPUT = new Text();

  private final String[] KEYS = new String[] { "Start Time", "Max Loudness",
      "Max Loudness Time", "Start Loudness" };

  private final int[] INDICES = new int[] { 18, 22, 23, 24 };

  /**
   * private final String[] KEYS = new String[] { "Start Time", "Pitch",
   * "Timbre", "Max Loudness", "Max Loudness Time", "Start Loudness" };
   * 
   * private final int[] INDICES = new int[] { 18, 20, 21, 22, 23, 24 };
   */
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
    for ( int i = 0; i < INDICES.length; i++ )
    {
      String val = itr.get( INDICES[ i ] ).trim();
      if ( !val.isEmpty() )
      {
        ID.set( KEYS[ i ] );
        OUTPUT.set( val );
        context.write( ID, OUTPUT );
      }
    }
  }
}
