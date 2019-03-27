package cs455.hadoop.artistloud;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.stream.Collectors;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer: Input to the reducer is the output from the mapper. It
 * receives word, list<count> pairs. Sums up individual counts per
 * given word. Emits <word, total count> pairs.
 */
public class MainReducer extends Reducer<Text, Text, Text, DoubleWritable> {

  private static final Map<Text, ArrayList<Double>> loudnessPerArtist =
      new HashMap<>();
  private static final Map<Text, Double> songsPerArtist = new HashMap<>();

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    Double loudness = null;
    Text name = null;
    for ( Text v : values )
    {
      try
      {
        loudness = Double.parseDouble( v.toString() );
      } catch ( NumberFormatException e )
      {
        name = new Text( v );
      }
    }
    if ( loudness != null && name != null )
    {
      // Increment song per artist if exists, else default to 1 song
      songsPerArtist.put( name, songsPerArtist.getOrDefault( name, 1.0 ) + 1 );

      // Append loudness of song to list for a given artist
      ArrayList<Double> list = loudnessPerArtist.get( name );
      if ( list == null )
      { // only create the list once
        list = new ArrayList<>();
        loudnessPerArtist.put( name, list );
      }
      list.add( loudness );
    } else
    {
      System.err.println( "Missing name or loudness for song_id" );
    }
  }

  @Override
  protected void cleanup(Context context)
      throws IOException, InterruptedException {

    topSongs( context, "\n-----Artist who has the most songs-----" );

    averageLoudness( context,
        "\n-----Artist's who's songs are the loudest on average-----" );
  }

  /**
   * Print out the top artist who has the most songs in the data set.
   * 
   * This requires sorting the <code>HashMap</code> songsPerArtist.
   * 
   * @param context
   * @param msg
   * @throws IOException
   * @throws InterruptedException
   */
  private void topSongs(Context context, String msg)
      throws IOException, InterruptedException {

    Map<Text, Double> sortedSongsPerArtist = sortMapByValue( songsPerArtist );

    context.write( new Text( msg ), new DoubleWritable() );

    writeMapToContext( context, sortedSongsPerArtist, 10 );
  }

  /**
   * Print out the artist's who's songs are the loudest on average.
   * 
   * This requires taking the average of the loudness values in the
   * <code>ArrayList</code> for a given artist in the
   * <code>HashMap</code> loudnessPerArtist.
   * 
   * @param context
   * @param msg
   * @throws IOException
   * @throws InterruptedException
   */
  private void averageLoudness(Context context, String msg)
      throws IOException, InterruptedException {
    final Map<Text, Double> avgLoudnessPerArtist = new HashMap<>();

    for ( Map.Entry<Text, ArrayList<Double>> entry : loudnessPerArtist
        .entrySet() )
    {
      OptionalDouble average =
          entry.getValue().stream().mapToDouble( a -> a ).average();

      avgLoudnessPerArtist.put( entry.getKey(),
          average.isPresent() ? average.getAsDouble() : -9999 );
    }
    Map<Text, Double> sortedLoudnessPerArtist =
        sortMapByValue( avgLoudnessPerArtist );

    context.write( new Text( msg ), new DoubleWritable() );

    writeMapToContext( context, sortedLoudnessPerArtist, 10 );
  }

  /**
   * Sort the map by value in descending order.
   * 
   * @param map
   * @return a new map of sorted <K, V> pairs.
   */
  private Map<Text, Double> sortMapByValue(Map<Text, Double> map) {
    return map.entrySet().stream()
        .sorted( Map.Entry.comparingByValue( Comparator.reverseOrder() ) )
        .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue,
            (e1, e2) -> e1, LinkedHashMap::new ) );
  }

  /**
   * Write out the first <code>numElements</code> to context.
   * 
   * @param context
   * @param map
   * @param numElements
   * @throws IOException
   * @throws InterruptedException
   */
  private void writeMapToContext(Context context, Map<Text, Double> map,
      int numElements) throws IOException, InterruptedException {
    int count = 0;
    for ( Text key : map.keySet() )
    {
      if ( count++ < numElements )
        context.write( key, new DoubleWritable( map.get( key ) ) );
      else
        break;
    }
  }

}
