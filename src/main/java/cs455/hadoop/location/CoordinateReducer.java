package cs455.hadoop.location;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CoordinateReducer extends Reducer<Text, Text, Text, Text> {

  private final Map<String, Integer> ITEMS = new HashMap<>();

  private final Set<Integer> YEARS = new HashSet<>();

  private final int METADATA_ITEMS = 4;

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    Integer year = null;
    String lon = "0.0";
    String lat = "0.0";

    for ( Text v : values )
    {
      String[] elements = v.toString().split( "\t" );
      final int nElements = elements.length;
      if ( nElements == METADATA_ITEMS )
      {
        if ( ( year = Integer.parseInt( elements[ 0 ] ) ) != null )
        {
          YEARS.add( year );
          if ( !( lon = elements[ 2 ] ).equals( "0.0" )
              && !( lat = elements[ 3 ] ).equals( "0.0" ) )
          {
            ITEMS.put( lon + "\t" + lat, year );
          }
        }
      }
    }
  }

  @Override
  protected void cleanup(Context context)
      throws IOException, InterruptedException {

    context.write( new Text( "0" ),
        new Text( Integer.toString( YEARS.size() ) + "\t0") );

    final Text out = new Text();

    final Text year = new Text();

    Comparator<Entry<String, Integer>> comparator = Collections
        .reverseOrder( (e1, e2) -> e1.getValue().compareTo( e2.getValue() ) );

    Map<String, Integer> sorted = ITEMS.entrySet().stream().sorted( comparator )
        .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue,
            (e1, e2) -> e2, LinkedHashMap::new ) );

    for ( Entry<String, Integer> e : sorted.entrySet() )
    {
      out.set( e.getKey() );
      year.set( Integer.toString( e.getValue() ) );
      context.write( out, year );
    }
  }
}
