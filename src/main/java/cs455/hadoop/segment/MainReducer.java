package cs455.hadoop.segment;

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
public class MainReducer extends Reducer<Text, Text, Text, Text> {

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

    for ( Text val : values )
    {
      String[] item = val.toString().split( "\\s+" );
      ITEMS.add( item );
      movingAverage.accept( item.length );
    }

    double[] averaged = computeAverage();
    StringBuilder sb = new StringBuilder();

    for ( int i = 0; i < averaged.length; ++i )
    {
      sb.append( averaged[ i ] ).append( " " );
    }
    context.write( key, new Text( sb.toString() + "\n" ) );
  }

  private double[] computeAverage() {
    int average = ( int ) Math.round( movingAverage.getAverage() );
    // int average = 900;
    double[] fin = new double[ average ];
    int[] indexSize = new int[ average ];

    for ( int i = 0; i < ITEMS.size(); ++i )
    {
      String[] item = ITEMS.get( i );
      int len = item.length;
      if ( len >= average )
      {
        int stride = len / average;
        for ( int j = 0; j < average; ++j )
        {
          fin[ j ] = updateAverage( fin[ j ], indexSize[ j ]++,
              DocumentUtilities.parseDouble( item[ j * stride ] ) );
        }
      } else
      {
        int stride = average / len;
        for ( int j = 0; j < len; ++j )
        {
          fin[ j * stride ] =
              updateAverage( fin[ j * stride ], indexSize[ j * stride ]++,
                  DocumentUtilities.parseDouble( item[ j ] ) );
        }
      }
    }
    return fin;
  }

  private double updateAverage(double average, int size, double value) {
    return ( size * average + value ) / ( size + 1 );
  }

}
