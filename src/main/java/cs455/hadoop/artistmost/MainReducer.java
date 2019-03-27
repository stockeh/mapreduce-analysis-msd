package cs455.hadoop.artistmost;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer: Input to the reducer is the output from the mapper. It
 * receives word, list<count> pairs. Sums up individual counts per
 * given word. Emits <word, total count> pairs.
 */
public class MainReducer
    extends Reducer<Text, IntWritable, Text, IntWritable> {

  private static Map<Text, Integer> tree = new HashMap<>();

  @Override
  protected void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {

    int sum = 0;
    for ( IntWritable val : values )
    {
      sum += val.get();
    }
    // TODO: Remove new Text?
    tree.put( new Text( key ), sum );
  }

  @Override
  protected void cleanup(Context context)
      throws IOException, InterruptedException {
    Map<Text, Integer> sortedMap = tree.entrySet().stream()
        .sorted( Map.Entry.comparingByValue( Comparator.reverseOrder() ) )
        .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue,
            (e1, e2) -> e1, LinkedHashMap::new ) );

    int count = 0;
    for ( Text key : sortedMap.keySet() )
    {
      if ( count++ < 10 )
        context.write( key, new IntWritable( sortedMap.get( key ) ) );
      else
        break;
    }
  }

}
