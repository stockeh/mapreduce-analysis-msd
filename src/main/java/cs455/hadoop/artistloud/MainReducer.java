package cs455.hadoop.artistloud;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.OptionalDouble;
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

  private static final Map<Text, ArrayList<Double>> tree = new HashMap<>();

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    Double loudness = null;
    Text name = null;
    for ( Text v : values )
    {
      try
      {
        loudness = Double.parseDouble( v.toString() );
      } catch ( NumberFormatException e )
      {
        name = new Text( v );
      }
    }
    if ( loudness != null && name != null )
    {
      ArrayList<Double> list = tree.get( name );
      if ( list == null )
      { // only create the list once
        list = new ArrayList<>();
        tree.put( name, list );
      }
      list.add( loudness );
    } else
    {
      System.err.println( "Missing name or loudness for song_id" );
    }
  }

  @Override
  protected void cleanup(Context context)
      throws IOException, InterruptedException {

    final Map<Text, Double> map = new HashMap<>();

    for ( Map.Entry<Text, ArrayList<Double>> entry : tree.entrySet() )
    {
      OptionalDouble average =
          entry.getValue().stream().mapToDouble( a -> a ).average();

      map.put( entry.getKey(),
          average.isPresent() ? average.getAsDouble() : -9999 );
    }
    Map<Text, Double> sortedMap = map.entrySet().stream()
        .sorted( Map.Entry.comparingByValue( Comparator.reverseOrder() ) )
        .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue,
            (e1, e2) -> e1, LinkedHashMap::new ) );

    int count = 0;
    for ( Text key : sortedMap.keySet() )
    {
      if ( count++ < 10 )
        context.write( key, new DoubleWritable( sortedMap.get( key ) ) );
      else
        break;
    }
  }

}
