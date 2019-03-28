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

  private static final Map<Text, Double> durationPerSong = new HashMap<>();

  private static double totalDuration = 0;

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    double hotness = 0;
    double duration = 0;
    Text songTitle = null;

    for ( Text v : values )
    {
      String[] elements = v.toString().split( "\t" );

      if ( elements.length == 2 )
      {
        try
        {
          hotness = Double.parseDouble( elements[ 0 ] );
          duration = Double.parseDouble( elements[ 1 ] );
        } catch ( NumberFormatException e )
        {
          System.err.println( "Defaulting hotness and duration to 0.0" );
        }
      } else
      {
        songTitle = new Text( v );
      }
    }

    durationPerSong.put( songTitle, duration );
    totalDuration += duration;

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

    songDuration( context,
        "\n-----Q5. Longest, shortest, and meadian length songs-----" );
  }

  private void songDuration(Context context, String msg)
      throws IOException, InterruptedException {
    context.write( new Text( msg ), new DoubleWritable() );

    context.write( new Text( "\n-----LONGEST-----" ), new DoubleWritable() );
    final Map<Text, Double> sortedDurationAscending =
        DocumentUtilities.sortMapByValue( durationPerSong, true );
    DocumentUtilities.writeMapToContext( context, sortedDurationAscending, 10 );

    context.write( new Text( "\n-----SHORTEST-----" ), new DoubleWritable() );
    final Map<Text, Double> sortedDurationDescending =
        DocumentUtilities.sortMapByValue( durationPerSong, false );
    DocumentUtilities.writeMapToContext( context, sortedDurationDescending,
        10 );
    context.write( new Text( "\n-----AVERAGE-----" ), new DoubleWritable() );
    context.write( new Text(),
        new DoubleWritable( totalDuration / durationPerSong.size() ) );
  }

  /**
   * Write out the songs with the highest hotness score.
   * 
   * This requires the <code>Map</code> of songs to be sorted in
   * descending order based on value.
   * 
   * @param context
   * @param msg
   * @throws IOException
   * @throws InterruptedException
   */
  private void topHotness(Context context, String msg)
      throws IOException, InterruptedException {
    final Map<Text, Double> sortedHotnessPerSong =
        DocumentUtilities.sortMapByValue( hotnessPerSong, true );

    context.write( new Text( msg ), new DoubleWritable() );

    DocumentUtilities.writeMapToContext( context, sortedHotnessPerSong, 10 );
  }
}
