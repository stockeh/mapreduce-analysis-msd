package cs455.hadoop.song;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
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

  private static final Map<Text, Double> dancergyPerSong = new HashMap<>();

  private static double totalDuration = 0;

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    double hotness = 0;
    double duration = 0;
    double dancergy = 0;

    Text songTitle = null;

    for ( Text v : values )
    {
      String[] elements = v.toString().split( "\t" );

      if ( elements.length == 4 )
      {
        hotness = DocumentUtilities.parseDouble( elements[ 0 ] );
        duration = DocumentUtilities.parseDouble( elements[ 1 ] );
        dancergy = DocumentUtilities.parseDouble( elements[ 2 ] )
            * DocumentUtilities.parseDouble( elements[ 3 ] );
      } else
      {
        songTitle = new Text( v );
      }
    }

    if ( hotness != 0 )
    {
      hotnessPerSong.put( songTitle, hotness );
    }
    if ( duration != 0 )
    {
      durationPerSong.put( songTitle, duration );
      totalDuration += duration;
    }
    if ( dancergy != 0 )
    {
      dancergyPerSong.put( songTitle, dancergy );
    }
  }

  @Override
  protected void cleanup(Context context)
      throws IOException, InterruptedException {

    topHotness( context,
        "\n-----Q3. Song with the highest hotness score-----" );

    songDuration( context,
        "\n-----Q5. Longest, shortest, and meadian length songs-----" );

    topDancergy( context,
        "\n-----Q6. Most energetic and danceabile songs-----" );
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

  /**
   * Display the longest, shortest, and songs of median length.
   * 
   * @param context
   * @param msg
   * @throws IOException
   * @throws InterruptedException
   */
  private void songDuration(Context context, String msg)
      throws IOException, InterruptedException {
    context.write( new Text( msg ), new DoubleWritable() );

    context.write( new Text( "\n-----LONGEST-----" ), new DoubleWritable() );
    Map<Text, Double> sorted =
        DocumentUtilities.sortMapByValue( durationPerSong, true );
    DocumentUtilities.writeMapToContext( context, sorted, 10 );

    context.write( new Text( "\n-----SHORTEST-----" ), new DoubleWritable() );
    sorted = DocumentUtilities.sortMapByValue( durationPerSong, false );
    DocumentUtilities.writeMapToContext( context, sorted, 10 );

    context.write( new Text( "\n-----AVERAGE-----" ), new DoubleWritable() );
    songDurationAverage( context );
  }

  /**
   * Find and display the 10 songs surrounding the average song duration
   * 
   * @param context
   * @throws IOException
   * @throws InterruptedException
   */
  private void songDurationAverage(Context context)
      throws IOException, InterruptedException {
    double average = totalDuration / durationPerSong.size();
    context.write( new Text( "Actual Average" ),
        new DoubleWritable( average ) );

    Map<Text, Double> averageMap = new HashMap<>();
    for ( Entry<Text, Double> entry : durationPerSong.entrySet() )
    {
      double value = entry.getValue();
      if ( value < average + 1 && value > average - 1 )
      {
        averageMap.put( entry.getKey(), value );
      }
      if ( averageMap.size() == 10 )
      {
        break;
      }
    }
    DocumentUtilities.writeMapToContext( context, averageMap, 10 );
  }

  /**
   * Display the top songs with the highest energy and danceability
   * measure.
   * 
   * @param context
   * @param msg
   * @throws IOException
   * @throws InterruptedException
   */
  private void topDancergy(Context context, String msg)
      throws IOException, InterruptedException {
    final Map<Text, Double> sorted =
        DocumentUtilities.sortMapByValue( dancergyPerSong, true );

    context.write( new Text( msg ), new DoubleWritable() );

    DocumentUtilities.writeMapToContext( context, sorted, 10 );
  }
}
