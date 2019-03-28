package cs455.hadoop.song;

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
   * < song_id , hotness duration >
   * 
   */
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    ArrayList<String> itr = DocumentUtilities.splitString( value.toString() );

    String id = itr.get( 1 );
    String hotness = itr.get( 2 );
    String duration = itr.get( 6 );

    if ( id.length() > 0 )
    {
      songID.set( id );
      outputValue.set( hotness + "\t" + duration );
      
      context.write( songID, outputValue );
    }
  }

}
