package cs455.hadoop.artist;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import cs455.hadoop.util.DocumentUtilities;

public class AnalysisMap extends Mapper<LongWritable, Text, Text, Text> {

  private final Text songID = new Text();

  private final Text outputValue = new Text();

  /**
   * Expected Output:
   * 
   * < song_id , loudness   fadeInDuration >
   * 
   */
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    ArrayList<String> itr = DocumentUtilities.splitString( value.toString() );

    String id = itr.get( 1 );
    String loudness = itr.get( 10 );
    String fadeInDuration = itr.get( 6 );
    // TODO: Verify fadeInDuration is a double..
    if ( id.length() > 0 )
    {
      songID.set( id );
      outputValue.set( loudness + "\t" + fadeInDuration );

      context.write( songID, outputValue );
    }
  }

}
