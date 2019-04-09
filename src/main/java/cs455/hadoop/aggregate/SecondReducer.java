package cs455.hadoop.aggregate;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;
import cs455.hadoop.util.DocumentUtilities;

/**
 * Reducer class that takes the output from the mapper and organizes
 * the values accordingly.
 *
 * Answers the following question:
 * 
 * <pre>
 * Create segment data for the average song.
 * Include start time, pitch, timbre, max loudness, max loudness time,
 * and start loudness
 * </pre>
 * 
 * Each Reducer is associated with a specific segment, and will read
 * the average length from cache.
 * 
 * @author stock
 *
 */
public class SecondReducer extends Reducer<Text, Text, Text, Text> {

  public static final List<String[]> TMP_ITEMS = new ArrayList<>();

  private static final Map<String, Integer> MAP = new HashMap<>();

  /**
   * Retrieve the cache files specified in the driver. This is executed
   * <b>once</b> before the map phase begings.
   */
  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    URI[] patternsURIs = Job.getInstance( conf ).getCacheFiles();
    for ( URI patternsURI : patternsURIs )
    {
      Path path = new Path( patternsURI.getPath() );
      String fileName = path.getName().toString();
      praseCacheFile( fileName );
    }
  }

  /**
   * Read in the distributed cache file looking for segment <i>Average
   * Identifier</i>, and their associated average values
   * 
   * It is assumed that the cache file has a line of the format:
   * 
   * <pre>
   * AVG_IDENTIFIER,###,###,###,###,###,###
   * </pre>
   * 
   * @param fileName to be parsed
   */
  private void praseCacheFile(String fileName) {
    try (
        BufferedReader fis = new BufferedReader( new FileReader( fileName ) ) )
    {
      String line = null;
      ArrayList<String> itr;
      while ( ( line = fis.readLine() ) != null )
      {
        itr = DocumentUtilities.splitString( line );

        if ( itr.size() == 7 )
        {
          if ( itr.get( 0 ).equals( DocumentUtilities.AVG_IDENTIFIER ) )
          {
            for ( int i = 0; i < itr.size() - 1; i++ )
            {
              MAP.put( DocumentUtilities.SEGMENT_KEYS[ i ],
                  Integer.parseInt( itr.get( i + 1 ) ) );
            }
          }
        }
      }
    } catch ( IOException e )
    {
      System.err.println( "Caught exception while parsing the cached file '"
          + StringUtils.stringifyException( e ) );
    }
  }

  /**
   * The mappers use the data's <b>column name</b> to join the data as a
   * <code>Text key</code>. The values are arranged in no particular
   * order, but differ in split size. Values from the
   * <code>AnalysisMap</code> are partitioned to the Reducer with the
   * associated <i>Average Identifier</i>.
   * 
   * Before the segment values are read for each <i>Average
   * Identifier</i>, the array size for the average segment length is
   * allocated as specified by the cache.
   */
  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    int size = MAP.get( key.toString() );
    double[] averaged = new double[ size ];;
    int[] indices = new int[ size ];;

    for ( Text val : values )
    {
      String[] item = val.toString().split( "\\s+" );
      combineSegment( averaged, indices, item );
    }

    StringBuilder sb = new StringBuilder();

    for ( int i = 0; i < averaged.length; ++i )
    {
      sb.append( averaged[ i ] ).append( " " );
    }
    context.write( key, new Text( sb.toString() ) );
  }

  /**
   * Combine the new segment value for some song to the <b>global</b>
   * average length segment.
   * 
   * Combination of the segments are accomplished via stride and
   * averaging techniques. This is done in one of two ways:
   * 
   * <ol>
   * <li>Longer segments map equally separated values to each value of
   * the globally averaged segment array</li>
   * <li>Shorter segments map each value to equally separated values of
   * the globally averaged segment array</li>
   * </ol>
   *
   * Upon mapping each value accordingly, the running average is updated
   * for the value of the globally averaged segment array.
   * 
   * @param averaged globally averaged segment array of length N (
   *        specified by cache value )
   * @param indices store the size ( number of elements added ) for each
   *        index of the globally averaged segment array
   * @param item segment data for some song
   */
  private void combineSegment(double[] averaged, int[] indices, String[] item) {

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

  /**
   * Update the average value for some index in the globally averaged
   * segment array.
   * 
   * @param average current value ( running average ) for some value the
   *        globally averaged segment array
   * @param size number of elements that have been added to a particular
   *        index
   * @param value new value to add to average
   * @return the new updated, average, value
   */
  private double updateAverage(double average, int size, double value) {
    return ( size * average + value ) / ( size + 1 );
  }

}
