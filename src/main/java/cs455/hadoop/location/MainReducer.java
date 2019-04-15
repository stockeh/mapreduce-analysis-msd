package cs455.hadoop.location;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer class that takes the output from the mapper and organizes
 * the values accordingly.
 * 
 * @author stock
 *
 */
public class MainReducer extends Reducer<Text, Text, Text, Text> {

  private final Map<String, Integer> ITEMS = new HashMap<>();

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    for ( Text val : values )
    {
      String v = val.toString();
      Integer count = ITEMS.get( v );
      if ( count == null )
      {
        ITEMS.put( v, 1 );
      } else
      {
        ITEMS.put( v, count + 1 );
      }
    }
  }

  @Override
  protected void cleanup(Context context)
      throws IOException, InterruptedException {

    final Text out = new Text();

    final Text freq = new Text();

    Comparator<Entry<String, Integer>> comparator = Collections
        .reverseOrder( (e1, e2) -> e1.getValue().compareTo( e2.getValue() ) );

    Map<String, Integer> sorted = ITEMS.entrySet().stream().sorted( comparator )
        .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue,
            (e1, e2) -> e2, LinkedHashMap::new ) );

    for ( Entry<String, Integer> e : sorted.entrySet() )
    {
      out.set( e.getKey() );
      freq.set( Integer.toString( e.getValue() ) );
      context.write( out, freq );
    }
  }
}
