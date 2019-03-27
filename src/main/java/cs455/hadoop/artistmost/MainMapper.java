package cs455.hadoop.artistmost;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1>
 * pairs.
 */
public class MainMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  private final String regexSplit = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String[] itr = value.toString().split( regexSplit );

    String artistName = itr[ 7 ];

    if ( artistName.length() > 0 )
    {
      context.write( new Text( artistName ), new IntWritable( 1 ) );
    }
  }
}
