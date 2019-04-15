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
      String song_id = itr.get( 8 );

      String year = itr.get( 14 ).trim();

      int y;
      try
      {
        y = Integer.parseInt( year );
      } catch ( NullPointerException | NumberFormatException e )
      {
        y = 0;
      }

      if ( !song_id.isEmpty() && y != 0 )
      {
        KEY.set( song_id );
        OUTPUT.set( sb.append( year ).append( "\t" )
            .append( DocumentUtilities.parseDouble( itr.get( 2 ) ) ) // artist_hotness
            .append( "\t" )
            .append( DocumentUtilities.parseDouble( itr.get( 5 ).trim() ) ) // lon
            .append( "\t" )
            .append( DocumentUtilities.parseDouble( itr.get( 4 ).trim() ) ) // lat
            .toString() );

        context.write( KEY, OUTPUT );
        sb.setLength( 0 );
      }
    }
  }
}
