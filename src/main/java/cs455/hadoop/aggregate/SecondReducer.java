package cs455.hadoop.aggregate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import cs455.hadoop.util.DocumentUtilities;

/**
 * Reducer class that takes the output from the mapper and organizes
 * the values accordingly.
 * 
 * @author stock
 *
 */
public class SecondReducer extends Reducer<Text, Text, Text, Text> {

  public static final List<String[]> TMP_ITEMS = new ArrayList<>();

  /**
   * The mappers use the data's <b>song_id</b> to join the data as a
   * <code>Text key</code>. The values are arranged in no particular
   * order, but differ in split size. Values from the
   * <code>AnalysisMap</code> class have 9 elements shown below. Data
   * from the <code>MetadataMap</code> only has 2 elements. This is used
   * to organize the values to their associated classes.
   */
  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    double[] averaged = null;
    int[] indices = null;

    for ( Text val : values )
    {
      String[] item = val.toString().split( "\\s+" );
      if ( averaged == null && item.length == 1 )
      {
        int size = Integer.parseInt( item[ 0 ] );
        averaged = new double[ size ];
        indices = new int[ size ];
      } else if ( averaged != null )
      {
        if ( !TMP_ITEMS.isEmpty() )
        {
          for ( String[] tmp : TMP_ITEMS )
          {
            computeAverage( averaged, indices, tmp );
          }
        } else
        {
          computeAverage( averaged, indices, item );
        }
      } else
      {
        TMP_ITEMS.add( item );
      }
    }
    StringBuilder sb = new StringBuilder();

    for ( int i = 0; i < averaged.length; ++i )
    {
      sb.append( averaged[ i ] ).append( " " );
    }
    context.write( key, new Text( sb.toString() + "\n" ) );
  }

  private void computeAverage(double[] averaged, int[] indices, String[] item) {

    int average = averaged.length;
    int len = item.length;

    if ( len >= average )
    {
      int stride = len / average;
      for ( int j = 0; j < average; ++j )
      {
        averaged[ j ] = updateAverage( averaged[ j ], indices[ j ]++,
            DocumentUtilities.parseDouble( item[ j * stride ] ) );
      }
    } else
    {
      int stride = average / len;
      for ( int j = 0; j < len; ++j )
      {
        averaged[ j * stride ] =
            updateAverage( averaged[ j * stride ], indices[ j * stride ]++,
                DocumentUtilities.parseDouble( item[ j ] ) );
      }
    }
  }

  private double updateAverage(double average, int size, double value) {
    return ( size * average + value ) / ( size + 1 );
  }

}
