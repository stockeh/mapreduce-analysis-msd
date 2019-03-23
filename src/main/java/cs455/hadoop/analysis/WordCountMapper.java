package cs455.hadoop.analysis;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1>
 * pairs.
 */
public class WordCountMapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    // tokenize into words.
    String[] itr = value.toString().split( "," );
    // emit word, count pairs.
    for ( int i = 0; i < itr.length; i++ )
    {
      context.write( new Text( itr[ i ] ), new IntWritable( 1 ) );
    }
  }
}
