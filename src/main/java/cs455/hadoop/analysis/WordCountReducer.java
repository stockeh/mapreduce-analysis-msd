package cs455.hadoop.analysis;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer: Input to the reducer is the output from the mapper. It
 * receives word, list<count> pairs. Sums up individual counts per
 * given word. Emits <word, total count> pairs.
 */
public class WordCountReducer
    extends Reducer<Text, IntWritable, Text, NullWritable> {

  private int topArtistCount = 0;
  private final Text topArtistName = new Text();

  @Override
  protected void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {

    int count = 0;
    for ( IntWritable val : values )
    {
      count += val.get();
    }
    // if ( values instanceof Collection )
    // {
    // count = ( ( Collection<?> ) values ).size();
    // }
    System.out.println( "HERE: " + count );

    if ( count > topArtistCount )
    {
      topArtistName.set( key );
    }
  }

  @Override
  protected void cleanup(Context context)
      throws IOException, InterruptedException {
    context.write( topArtistName, NullWritable.get() );

  }
}
