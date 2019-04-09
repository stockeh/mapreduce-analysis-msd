package cs455.hadoop.aggregate;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import cs455.hadoop.util.DocumentUtilities;

public class CustomPartitioner extends Partitioner<Text, Text> {

  public static final Text StartTime =
      new Text( DocumentUtilities.SEGMENT_KEYS[ 0 ] );

  public static final Text Pitch =
      new Text( DocumentUtilities.SEGMENT_KEYS[ 1 ] );

  public static final Text Timbre =
      new Text( DocumentUtilities.SEGMENT_KEYS[ 2 ] );

  public static final Text MaxLoudness =
      new Text( DocumentUtilities.SEGMENT_KEYS[ 3 ] );

  public static final Text MaxLoudnessTime =
      new Text( DocumentUtilities.SEGMENT_KEYS[ 4 ] );

  @Override
  public int getPartition(Text key, Text value, int numReduceTasks) {
    if ( numReduceTasks == 0 )
    {
      return 0;
    } else if ( key.equals( StartTime ) )
    {
      return 0;
    } else if ( key.equals( Pitch ) )
    {
      return 1;
    } else if ( key.equals( Timbre ) )
    {
      return 2;
    } else if ( key.equals( MaxLoudness ) )
    {
      return 3;
    } else if ( key.equals( MaxLoudnessTime ) )
    {
      return 4;
    } else
    {
      return 5;
    }

  }
}
