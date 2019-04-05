package cs455.hadoop.segment;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MainCombiner extends Reducer<Text, Text, Text, Text> {
  
  /**
   * Combiner class to reduce the data at node level
   */
  public void reduce(Text key, Iterable<Text> value, Context context)
      throws IOException, InterruptedException {

    context.write( key, new Text() );
  }

}
