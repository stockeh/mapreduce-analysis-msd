package cs455.hadoop.artist;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import cs455.hadoop.util.DocumentUtilities;

public class MetadataMap extends Mapper<LongWritable, Text, Text, Text> {

  private final Text songID = new Text();

  private final Text artistName = new Text();

  /**
   * Expected Output:
   * 
   * < song_id , artist_name >
   * 
   */
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    ArrayList<String> itr = DocumentUtilities.splitString( value.toString() );

    String id = itr.get( 8 );
    String name = itr.get( 7 );

    if ( id.length() > 0 && name.length() > 0 )
    {
      songID.set( id );
      artistName.set( name );
      
      context.write( songID, artistName );
    }
  }
}
