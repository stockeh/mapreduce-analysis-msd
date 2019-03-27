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
    context.write( new Text( itr[ 1 ] ), new Text( itr[ 10 ] ) );
  }

}
