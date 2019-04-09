package cs455.hadoop.aggregate;

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
public class AnalysisMap extends Mapper<LongWritable, Text, Text, Text> {

  private final Text songID = new Text();

  private final Text outputValue = new Text();

  private final StringBuilder sb = new StringBuilder();

  /**
   * Expected Output:
   * 
   * < song_id , hotness danceability duration fade_in energy key
   * loudness mode fade_out tempo time_signature e energy >
   * 
   */
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    ArrayList<String> itr = DocumentUtilities.splitString( value.toString() );

    String id = itr.get( 1 );

    if ( !id.isEmpty() && !id.equals( "song_id" ))
    {
      songID.set( id );

      for ( int i = 0; i < DocumentUtilities.SEGMENT_INDICES.length; i++ )
      {
        int length = itr.get( DocumentUtilities.SEGMENT_INDICES[ i ] )
            .split( "\\s+" ).length;
        sb.append( Integer.toString( length ) ).append( "\t" );
      }
      outputValue.set( sb.toString() );
      sb.setLength( 0 );
      context.write( songID, outputValue );

      String hotness = itr.get( 2 );

      double hot;
      try
      {
        hot = Double.parseDouble( hotness );
      } catch ( NullPointerException | NumberFormatException e )
      {
        hot = 0;
      }
      
      if ( hot != 0 )
      {
        sb.append( hotness ).append( "\t" ); // hotness
        // sb.append( itr.get( 4 ) ).append( "\t" ); // danceability
        sb.append( itr.get( 5 ) ).append( "\t" ); // duration
        sb.append( itr.get( 6 ) ).append( "\t" ); // fade_in
        // sb.append( itr.get( 7 ) ).append( "\t" ); // energy
        sb.append( itr.get( 8 ) ).append( "\t" ); // key
        sb.append( itr.get( 10 ) ).append( "\t" ); // loudness
        sb.append( itr.get( 11 ) ).append( "\t" ); // mode
        sb.append( itr.get( 13 ) ).append( "\t" ); // fade_out
        sb.append( itr.get( 14 ) ).append( "\t" ); // tempo
        sb.append( itr.get( 15 ) ).append( " " ); // time_signature
        
        outputValue.set( sb.toString() );
        sb.setLength( 0 );
        context.write( songID, outputValue );
      }
    }
  }

}
