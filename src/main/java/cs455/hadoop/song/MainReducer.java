package cs455.hadoop.song;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import cs455.hadoop.util.DocumentUtilities;

/**
 * Reducer: Input to the reducer is the output from the mapper. It
 * receives word, list<count> pairs. Sums up individual counts per
 * given word. Emits <word, total count> pairs.
 */
public class MainReducer extends Reducer<Text, Text, Text, DoubleWritable> {

  private static final Map<Text, Double> hotnessPerSong = new HashMap<>();

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    Double hotness = new Double( 0 );
    // Double duration = new Double( 0 );
    Text songTitle = null;

    for ( Text v : values )
    {
      String[] elements = v.toString().split( "\t" );

      if ( elements.length == 2 )
      {
        try
        {
          hotness = Double.parseDouble( elements[ 0 ] );
          // duration = Double.parseDouble( elements[ 1 ] );
        } catch ( NumberFormatException e )
        {
          System.err.println( "Defaulting hotness and duration to 0.0" );
        }
      } else
      {
        songTitle = new Text( v );
      }
    }
    if ( hotness != 0 )
    {
      hotnessPerSong.put( songTitle, hotness );
    }
  }

  @Override
  protected void cleanup(Context context)
      throws IOException, InterruptedException {

    topHotness( context,
        "\n-----Q3. Song with the highest hotness score-----" );
  }

  private void topHotness(Context context, String msg)
      throws IOException, InterruptedException {
    final Map<Text, Double> sortedHotnessPerSong =
        DocumentUtilities.sortMapByValue( hotnessPerSong );

    context.write( new Text( msg ), new DoubleWritable() );

    DocumentUtilities.writeMapToContext( context, sortedHotnessPerSong, 10 );
  }
}
