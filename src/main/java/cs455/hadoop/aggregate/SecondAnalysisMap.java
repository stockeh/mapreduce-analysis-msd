package cs455.hadoop.aggregate;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import cs455.hadoop.util.DocumentUtilities;

public class SecondAnalysisMap extends Mapper<LongWritable, Text, Text, Text> {

  private final Text ID = new Text();

  private final Text OUTPUT = new Text();

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
      for ( int i = 0; i < DocumentUtilities.SEGMENT_INDICES.length; i++ )
      {
        String val = itr.get( DocumentUtilities.SEGMENT_INDICES[ i ] ).trim();
        if ( !val.isEmpty() )
        {
          ID.set( DocumentUtilities.SEGMENT_KEYS[ i ] );
          OUTPUT.set( val );
          context.write( ID, OUTPUT );
        }
      }
    }
  }
}
