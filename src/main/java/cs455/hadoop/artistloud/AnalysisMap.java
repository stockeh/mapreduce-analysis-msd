package cs455.hadoop.artistloud;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AnalysisMap extends Mapper<LongWritable, Text, Text, Text> {

  private final String regexSplit = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

  /**
   * Expected Output:
   * 
   * < song_id , loudness >
   * 
   */
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String[] itr = value.toString().split( regexSplit );

    String songID = itr[ 1 ];
    String loudness = itr[ 10 ];

    if ( songID.length() > 0 && loudness.length() > 0 )
    {
      context.write( new Text( songID ), new Text( loudness ) );
    }
  }

}
