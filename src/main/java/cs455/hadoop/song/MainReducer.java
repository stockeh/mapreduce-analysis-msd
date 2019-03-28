package cs455.hadoop.song;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer: Input to the reducer is the output from the mapper. It
 * receives word, list<count> pairs. Sums up individual counts per
 * given word. Emits <word, total count> pairs.
 */
public class MainReducer extends Reducer<Text, Text, Text, DoubleWritable> {

  private static final Map<Text, Double> hotnessPerSong = new HashMap<>();

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    Double hotness = new Double( 0 );
    // Double duration = new Double( 0 );
    Text songTitle = null;

    for ( Text v : values )
    {
      String[] elements = v.toString().split( "\t" );

      if ( elements.length == 2 )
      {
        try
        {
          hotness = Double.parseDouble( elements[ 0 ] );
          // duration = Double.parseDouble( elements[ 1 ] );
        } catch ( NumberFormatException e )
        {
          System.err.println( "Defaulting hotness and duration to 0.0" );
        }
      } else
      {
        songTitle = new Text( v );
      }
    }
    if ( hotness != 0 )
    {
      hotnessPerSong.put( songTitle, hotness );
    }
  }

  @Override
  protected void cleanup(Context context)
      throws IOException, InterruptedException {

    topHotness( context,
        "\n-----Q3. Song with the highest hotness score-----" );
  }

  private void topHotness(Context context, String msg)
      throws IOException, InterruptedException {
    final Map<Text, Double> sortedHotnessPerSong =
        sortMapByValue( hotnessPerSong );

    context.write( new Text( msg ), new DoubleWritable() );

    writeMapToContext( context, sortedHotnessPerSong, 10 );
  }

  /**
   * Sort the map by value in descending order.
   * 
   * @param map
   * @return a new map of sorted <K, V> pairs.
   */
  private Map<Text, Double> sortMapByValue(Map<Text, Double> map) {
    return map.entrySet().stream()
        .sorted( Map.Entry.comparingByValue( Comparator.reverseOrder() ) )
        .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue,
            (e1, e2) -> e1, LinkedHashMap::new ) );
  }

  /**
   * Write out the first <code>numElements</code> to context.
   * 
   * @param context
   * @param map
   * @param numElements
   * @throws IOException
   * @throws InterruptedException
   */
  private void writeMapToContext(Context context, Map<Text, Double> map,
      int numElements) throws IOException, InterruptedException {

    int count = 0;
    for ( Text key : map.keySet() )
    {
      if ( count++ < numElements )
        context.write( key, new DoubleWritable( map.get( key ) ) );
      else
        break;
    }
  }

}
