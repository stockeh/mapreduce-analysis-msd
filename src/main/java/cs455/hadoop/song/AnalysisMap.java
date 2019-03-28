package cs455.hadoop.song;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AnalysisMap extends Mapper<LongWritable, Text, Text, Text> {

  private final String regexSplit = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

  private final Text songID = new Text();

  private final Text outputValue = new Text();

  /**
   * Expected Output:
   * 
   * < song_id , hotness    duration >
   * 
   */
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String[] itr = value.toString().split( regexSplit );

    String id = itr[ 1 ];
    String hotness = itr[ 2 ];
    String duration = itr[ 6 ];

    if ( id.length() > 0 )
    {
      songID.set( id );
      outputValue.set( hotness + "\t" + duration );
      context.write( songID, outputValue );
    }
  }

}
