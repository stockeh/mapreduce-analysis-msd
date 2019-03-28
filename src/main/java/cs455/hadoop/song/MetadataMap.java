package cs455.hadoop.song;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MetadataMap extends Mapper<LongWritable, Text, Text, Text> {

  private final String regexSplit = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

  private final Text songID = new Text();
  
  private final Text songTitle  = new Text();
  
  /**
   * Expected Output:
   * 
   * < song_id , song_title >
   * 
   */
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    String[] itr = value.toString().split( regexSplit );
    
    String id = itr[ 8 ];
    String title = itr[ 9 ];

    if ( id.length() > 0 && title.length() > 0 )
    {
      songID.set( id );
      songTitle.set( title );
      context.write( songID, songTitle );
    }
  }
}
