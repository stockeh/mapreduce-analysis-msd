package cs455.hadoop.location;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer class that takes the output from the mapper and organizes
 * the values accordingly.
 * 
 * @author stock
 *
 */
public class MainReducer extends Reducer<Text, Text, Text, NullWritable> {

  private final Map<String, List<String[]>> ITEMS = new HashMap<>();

  private final String[] INDICES = new String[] { "1:", "2:", "3:", "4:", "5:",
      "6:", "7:", "8:", "9:", "10:" };

  private final int ANALYSIS_ITEMS = 9;

  private final int METADATA_ITEMS = 4;

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    int it = 0;
    String year = null;
    String[] features = new String[ 10 ];

    for ( Text v : values )
    {
      String[] elements = v.toString().split( "\t" );
      final int nElements = elements.length;
      if ( nElements == ANALYSIS_ITEMS )
      {
        for ( int i = 0; i < ANALYSIS_ITEMS; i++ )
        {
          features[ i ] = INDICES[ i ] + elements[ i ];
        }
        ++it;
      } else if ( nElements == METADATA_ITEMS )
      {
        year = elements[ 0 ];
        features[ 9 ] = INDICES[ 9 ] + elements[ 1 ];
        ++it;
      }
    }
    if ( it == 2 && year != null )
    {
      List<String[]> list = ITEMS.get( year );
      if ( list == null )
      {
        list = new ArrayList<>();
        ITEMS.put( year, list );
      }
      list.add( features );
    }
  }

  @Override
  protected void cleanup(Context context)
      throws IOException, InterruptedException {

    final Text out = new Text();
    StringBuilder sb = new StringBuilder();

    for ( Entry<String, List<String[]>> e : ITEMS.entrySet() )
    {
      for ( String[] sample : e.getValue() )
      {
        sb.append( e.getKey() );
        for ( int i = 0; i < sample.length; ++i )
        {
          sb.append( " " ).append( sample[ i ] );
        }
        out.set( sb.toString() );
        sb.setLength( 0 );
        context.write( out, NullWritable.get() );
      }
    }
  }
}
