package cs455.hadoop.location;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import cs455.hadoop.util.DocumentUtilities;

/**
 * Mapper class for the analysis data files.
 * 
 * @author stock
 *
 */
public class MetadataMap extends Mapper<LongWritable, Text, Text, Text> {

  private final Text KEY = new Text();

  private final Text OUTPUT = new Text();

  private final StringBuilder sb = new StringBuilder();

  /**
   * Expected Output:
   * 
   * < artist_id, longitude latitude >
   * 
   */
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    ArrayList<String> itr = DocumentUtilities.splitString( value.toString() );
    if ( !itr.get( 0 ).isEmpty() )
    {
      String id = itr.get( 3 );

      String lat = itr.get( 4 ).trim();
      String lon = itr.get( 5 ).trim();

      if ( !id.isEmpty() && !lat.isEmpty() && !lon.isEmpty() )
      {
        KEY.set( id );
        OUTPUT.set( sb.append( lon ).append( "\t" ).append( lat ).toString() );

        context.write( KEY, OUTPUT );

        sb.setLength( 0 );
      }
    }
  }
}
