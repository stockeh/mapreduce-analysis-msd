package cs455.hadoop.segment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;
import java.util.List;
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

    context.write( key,
        new Text( Double.toString( movingAverage.getAverage() ) ) );
  }
}
