package cs455.hadoop.aggregate;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
 * @author stock
 *
 */
public class SecondReducer extends Reducer<Text, Text, Text, Text> {

  public static final List<String[]> TMP_ITEMS = new ArrayList<>();

  private static final Map<String, Integer> MAP = new HashMap<>();

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

    int size = MAP.get( key.toString() );
    double[] averaged = new double[ size ];;
    int[] indices = new int[ size ];;

    for ( Text val : values )
    {
      String[] item = val.toString().split( "\\s+" );
      computeAverage( averaged, indices, item );
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
