package cs455.hadoop.aggregate;

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
   * < song_id , terms_descriptions_freq terms_descriptions_weight >
   * 
   */
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    ArrayList<String> itr = DocumentUtilities.splitString( value.toString() );

    String id = itr.get( 8 );

    if ( !id.isEmpty() )
    {

      sb.append( itr.get( 12 ) ); // terms_descriptions_freq
      sb.append( "\t" );
      sb.append( itr.get( 13 ).trim() ); // terms_descriptions_weight
      sb.append( " " );
      
      songID.set( id );
      outputValue.set( sb.toString() );
      sb.setLength( 0 );

      context.write( songID, outputValue );
    }
  }
}
