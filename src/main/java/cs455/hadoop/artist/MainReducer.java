package cs455.hadoop.artist;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalDouble;
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

  private static final Map<Text, Double> songsPerArtist = new HashMap<>();

  private static final Map<Text, ArrayList<Double>> loudnessPerArtist =
      new HashMap<>();

  private static final Map<Text, Double> fadeDurationPerArtist =
      new HashMap<>();

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    Double loudness = new Double( 0 );
    Double fadeInDuration = new Double( 0 );
    Text name = null;
    for ( Text v : values )
    {
      String[] elements = v.toString().split( "\t" );

      if ( elements.length == 2 )
      {
        try
        {
          loudness = Double.parseDouble( elements[ 0 ] );
          fadeInDuration = Double.parseDouble( elements[ 1 ] );
        } catch ( NumberFormatException e )
        {
          System.err.println( "Defaulting loudness and fadeInDuration to 0.0" );
        }
      } else
      {
        name = new Text( v );
      }
    }
    if ( name != null )
    {
      // Increment song per artist if exists, else default to 1 song
      songsPerArtist.put( name, songsPerArtist.getOrDefault( name, 0.0 ) + 1 );

      // Sum all fadeInDuration times for an artist
      fadeDurationPerArtist.put( name,
          songsPerArtist.getOrDefault( name, 0.0 ) + fadeInDuration );

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
      System.err.println( "Missing name for song_id" );
    }
  }

  @Override
  protected void cleanup(Context context)
      throws IOException, InterruptedException {

    topSongs( context, "\n-----Q1. Artist who has the most songs-----" );

    averageLoudness( context,
        "\n-----Q2. Artist's who's songs are the loudest on average-----" );

    totalFadeIn( context,
        "\n-----Q4. Artist with highest total time spent fading in songs-----" );
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
    final Map<Text, Double> sortedSongsPerArtist =
        DocumentUtilities.sortMapByValue( songsPerArtist );

    context.write( new Text( msg ), new DoubleWritable() );

    DocumentUtilities.writeMapToContext( context, sortedSongsPerArtist, 10 );
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
    final Map<Text, Double> sortedLoudnessPerArtist =
        DocumentUtilities.sortMapByValue( avgLoudnessPerArtist );

    context.write( new Text( msg ), new DoubleWritable() );

    DocumentUtilities.writeMapToContext( context, sortedLoudnessPerArtist, 10 );
  }


  /**
   * Print out the artist that has the highest total time spend fading
   * in their songs.
   * 
   * This requires sorting the <code>HashMap</code>
   * fadeDurationPerArtist.
   * 
   * @param context
   * @param string
   * @throws InterruptedException
   * @throws IOException
   */
  private void totalFadeIn(Context context, String msg)
      throws IOException, InterruptedException {
    final Map<Text, Double> sortedSongsPerArtist =
        DocumentUtilities.sortMapByValue( fadeDurationPerArtist );

    context.write( new Text( msg ), new DoubleWritable() );

    DocumentUtilities.writeMapToContext( context, sortedSongsPerArtist, 10 );
  }

}
