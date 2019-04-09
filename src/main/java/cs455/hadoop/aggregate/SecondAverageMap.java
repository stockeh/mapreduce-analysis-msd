package cs455.hadoop.aggregate;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import cs455.hadoop.util.DocumentUtilities;

public class SecondAverageMap extends Mapper<LongWritable, Text, Text, Text> {

  private final Text ID = new Text();

  private final Text OUTPUT = new Text();


  /**
   * Emit the averages for each key as computed in JOB 1.
   * 
   * The line shall be of the form:
   * 
   * <pre>
   * AVG_IDENTIFIER,#.###,#.###,#.###,#.###,#.###,#.###
   * </pre>
   * 
   */
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    ArrayList<String> itr = DocumentUtilities.splitString( value.toString() );

    if ( itr.size() == 7 )
    {
      if ( itr.get( 0 ).equals( DocumentUtilities.AVG_IDENTIFIER ) )
      {
        for ( int i = 1; i < itr.size(); i++ )
        {
          String val = itr.get( i ).trim();
          if ( !val.isEmpty() )
          {
            ID.set( DocumentUtilities.SEGMENT_KEYS[ i ] );
            OUTPUT.set( val );
            context.write( ID, OUTPUT );
          }
        }
      }
    }
  }
}
