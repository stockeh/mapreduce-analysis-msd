package cs455.hadoop.aggregate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;
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

  public static final List<String[]> ITEMS = new ArrayList<>();

  public static final DoubleSummaryStatistics movingAverage =
      new DoubleSummaryStatistics();

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
    int average = 200;
    double[] averaged = new double[ average ];
    int[] indices = new int[ average ];

    boolean first = true;
    int index = 0;
    for ( Text val : values )
    {
      if ( first )
      {
        context.write( val, new Text( Integer.toString( index ) ) );
        first = false;
      } else
      {
        break;
      }
      ++index;
      // String[] item = val.toString().split( "\\s+" );
      // computeAverage( averaged, indices, item );
    }
    context.write( key, new Text( "MADE IT OUT" ) );
    // StringBuilder sb = new StringBuilder();
    //
    // for ( int i = 0; i < averaged.length; ++i )
    // {
    // sb.append( averaged[ i ] ).append( " " );
    // }
    // context.write( key, new Text( sb.toString() + "\n" ) );
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
