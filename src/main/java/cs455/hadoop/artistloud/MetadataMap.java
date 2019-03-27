package cs455.hadoop.artistloud;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MetadataMap extends Mapper<LongWritable, Text, Text, Text> {

  private final String regexSplit = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

  /**
   * Expected Output:
   * 
   * < song_id , artists_name >
   * 
   */
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    String[] itr = value.toString().split( regexSplit );
    
    String songID = itr[ 8 ];
    String artistName = itr[ 7 ];

    if ( songID.length() > 0 && artistName.length() > 0 )
    {
      context.write( new Text( songID ), new Text( artistName ) );
    }
  }
}
