package cs455.hadoop.basic;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import cs455.hadoop.util.DocumentUtilities;

/**
 * Mapper class for the metadata files.
 * 
 * @author stock
 *
 */
public class MetadataMap extends Mapper<LongWritable, Text, Text, Text> {

  private final Text songID = new Text();

  private final Text outputValue = new Text();

  private final StringBuilder sb = new StringBuilder();

  /**
   * Expected Output:
   * 
   * < song_id , song_title song_artist >
   * 
   */
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    ArrayList<String> itr = DocumentUtilities.splitString( value.toString() );

    String id = itr.get( 8 );

    if ( id.length() > 0 )
    {

      sb.append( itr.get( 9 ) ); // song_title
      sb.append( "\t" );
      sb.append( itr.get( 7 ).trim() ); // song_artist
      sb.append( "_" );

      songID.set( id );
      outputValue.set( sb.toString() );
      sb.setLength( 0 );

      context.write( songID, outputValue );
    }
  }
}
