package cs455.hadoop.basic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import cs455.hadoop.items.ArtistData;
import cs455.hadoop.items.Data;
import cs455.hadoop.items.Item;
import cs455.hadoop.items.Song;
import cs455.hadoop.items.SongData;
import cs455.hadoop.util.DocumentUtilities;

/**
 * Reducer class that takes the output from the mapper and organizes
 * the values accordingly.
 * 
 * @author stock
 *
 */
public class MainReducer extends Reducer<Text, Text, Text, DoubleWritable> {

  private static final Map<Text, Item> songs = new HashMap<>();

  private static final Map<Text, Item> artists = new HashMap<>();

  private static final Map<String, Double> ranks = new HashMap<>();

  private static final Map<String, List<String>> links = new HashMap<>();

  /**
   * The mappers use the data's <b>song_id</b> to join the data as a
   * <code>Text key</code>. The values are arranged in no particular
   * order, but differ in split size. Values from the
   * <code>AnalysisMap</code> class have 6 elements shown below. Data
   * from the <code>MetadataMap</code> only has 2 elements. This is used
   * to organize the values to their associated classes.
   */
  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    double loudness = 0;
    double fade = 0;
    double hotness = 0;
    double duration = 0;
    double dancergy = 0;

    Text songTitle = null;
    Text artistName = null;

    String artistID = null;
    List<String> similarArtists = null;

    for ( Text v : values )
    {
      String[] elements = v.toString().split( "\t" );

      if ( elements.length == 6 )
      {
        loudness = DocumentUtilities.parseDouble( elements[ 0 ] );
        fade = DocumentUtilities.parseDouble( elements[ 1 ] );
        hotness = DocumentUtilities.parseDouble( elements[ 2 ] );
        duration = DocumentUtilities.parseDouble( elements[ 3 ] );
        dancergy = DocumentUtilities.parseDouble( elements[ 4 ] )
            * DocumentUtilities.parseDouble( elements[ 5 ] );
      } else
      {
        songTitle = new Text( elements[ 0 ] );
        artistName = new Text( elements[ 1 ] );
        artistID = elements[ 3 ];
        similarArtists =
            new ArrayList<>( Arrays.asList( elements[ 2 ].split( "\\s+" ) ) );
      }
    }

    if ( artistName != null )
    {
      DocumentUtilities.addData( artists, ArtistData.TOTAL_SONGS, artistName,
          0 );

      DocumentUtilities.addData( artists, ArtistData.LOUDNESS, artistName,
          loudness );

      DocumentUtilities.addData( artists, ArtistData.FADE_DURATION, artistName,
          fade );

    }
    if ( songTitle != null )
    {
      DocumentUtilities.addData( songs, SongData.HOTNESS, songTitle, hotness );

      DocumentUtilities.addData( songs, SongData.DURATION, songTitle,
          duration );

      DocumentUtilities.addData( songs, SongData.DANCE_ENERGY, songTitle,
          dancergy );
    }
    ranks.put( artistID, 1.0 );
    // Set first item in similarity list to artistName - this is excluded
    // in computations, and just used for printing.
    similarArtists.add( 0, artistName.toString() );
    links.put( artistID, similarArtists );
  }

  /**
   * Once the <code>reduce</code> method has completed execution, the
   * <code>cleanup</code> method is called.
   * 
   * This method will write associated values in the desired manner to
   * <code>Context</code> to be viewed in HDFS.
   */
  @Override
  protected void cleanup(Context context)
      throws IOException, InterruptedException {

    context.write( new Text( "\n----Q1. ARTIST WHO HAS THE MOST SONGS" ),
        new DoubleWritable() );
    top( context, artists, ArtistData.TOTAL_SONGS, true );

    context.write(
        new Text( "\n----Q2. ARTIST'S WHO'S SONGS ARE THE LOUDEST ON AVERAGE" ),
        new DoubleWritable() );
    top( context, artists, ArtistData.LOUDNESS, true );

    context.write( new Text( "\n----Q3. SONGS WITH THE HIGHEST HOTNESS SCORE" ),
        new DoubleWritable() );
    top( context, songs, SongData.HOTNESS, true );

    context.write(
        new Text(
            "\n----Q4. ARTISTS WITH HIGHEST TOTAL TIME SPENT FADING IN SONGS" ),
        new DoubleWritable() );
    top( context, artists, ArtistData.FADE_DURATION, true );

    context.write(
        new Text( "\n----Q5. LONGEST, SHORTEST, AND MEDIAN LENGTH SONGS" ),
        new DoubleWritable() );
    context.write( new Text( "\n----LONGEST----" ), new DoubleWritable() );
    top( context, songs, SongData.DURATION, true );

    context.write( new Text( "\n----SHORTEST----" ), new DoubleWritable() );
    top( context, songs, SongData.DURATION, false );

    context.write( new Text( "\n----AVERAGE----" ), new DoubleWritable() );
    songDurationAverage( context );

    context.write( new Text( "\n----MEDIAN----" ), new DoubleWritable() );
    songDurationMedian( context );

    context.write( new Text( "\n----Q6. MOST ENERGETIC AND DANCEABILE SONGS" ),
        new DoubleWritable() );
    top( context, songs, SongData.DANCE_ENERGY, true );

    context.write( new Text( "\n----Q8. MOST GENERIC / UNIQUE ARTIST" ),
        new DoubleWritable() );
    pageRank( context );

  }

  /**
   * Compute page rank of artists and list of similar artists.
   * 
   * @param context
   * @throws IOException
   * @throws InterruptedException
   */
  private void pageRank(Context context)
      throws IOException, InterruptedException {
    int iters = 10;

    while ( iters-- > 0 )
    {
      Map<String, Double> contrib = new HashMap<>();
      for ( Entry<String, Double> entry : ranks.entrySet() )
      {
        String key = entry.getKey();
        List<String> urls = links.get( key );
        List<String> modified = new ArrayList<>();

        for ( String url : urls.subList( 1, urls.size() ) )
        {
          if ( ranks.containsKey( url ) )
          {
            modified.add( url );
          }
        }
        Double contribValue = entry.getValue() / modified.size();

        for ( String url : modified )
        {
          contrib.put( url, contrib.getOrDefault( url, 0.0 ) + contribValue );
        }
      }
      for ( String key : ranks.keySet() )
      {
        ranks.put( key, 0.15 + 0.85 * contrib.getOrDefault( key, -0.175 ) );
      }
    }

    Map<String, Double> sorted = ranks.entrySet().stream()
        .sorted( Collections.reverseOrder( Map.Entry.comparingByValue() ) )
        .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue,
            (e1, e2) -> e2, LinkedHashMap::new ) );
    int count = 0;
    for ( Entry<String, Double> entry : sorted.entrySet() )
    {
      if ( count++ < 10 )
      {
        context.write( new Text( links.get( entry.getKey() ).get( 0 ) ),
            new DoubleWritable( entry.getValue() ) );
      } else
      {
        break;
      }
    }
  }

  /**
   * Write out the items with the highest hotness score.
   * 
   * This requires the <code>Map</code> to be sorted in either
   * descending or ascending order based on value.
   * 
   * @param context
   * @param msg to be sorted and displayed based on data type
   * @throws IOException
   * @throws InterruptedException
   */
  private void top(Context context, Map<Text, Item> map, final Data type,
      boolean decending) throws IOException, InterruptedException {

    final Map<Text, Item> sorted =
        DocumentUtilities.sortMapByValue( map, type, decending );

    DocumentUtilities.writeMapToContext( context, sorted, type, 10 );
  }

  /**
   * 
   * @param context
   * @throws IOException
   * @throws InterruptedException
   */
  private void songDurationMedian(Context context)
      throws IOException, InterruptedException {

    final Data type = SongData.DURATION;

    final int median = ( int ) Song.totalSongsOfDuration / 2;

    final Map<Text, Item> sorted =
        DocumentUtilities.sortMapByValue( songs, type, true );

    int index = 0;
    for ( Entry<Text, Item> entry : sorted.entrySet() )
    {
      if ( index - 5 > median )
      {
        break;
      }
      if ( index + 5 > median )
      {
        Item item = entry.getValue();
        if ( type.isInvalid( item ) )
        {
          context.write( entry.getKey(),
              new DoubleWritable( type.getValue( item ) ) );
        } else
        {
          index--;
        }
      }
      index++;
    }
  }

  /**
   * Find and display the 10 songs surrounding the average and median
   * song duration.
   * 
   * @param context
   * @throws IOException
   * @throws InterruptedException
   */
  private void songDurationAverage(Context context)
      throws IOException, InterruptedException {

    double average = Song.totalDuration / Song.totalSongsOfDuration;
    context.write( new Text( "Actual Average" ),
        new DoubleWritable( average ) );

    final Data type = SongData.DURATION;

    Map<Text, Item> averageMap = new HashMap<>();

    for ( Entry<Text, Item> entry : songs.entrySet() )
    {
      Item item = entry.getValue();
      if ( type.isInvalid( item ) )
      {
        double value = ( ( Song ) item ).getDuration();
        if ( value < average + 1 && value > average - 1 )
        {
          averageMap.put( entry.getKey(), item );
        }
        if ( averageMap.size() == 10 )
        {
          break;
        }
      }
    }
    DocumentUtilities.writeMapToContext( context, averageMap, type, 10 );
  }
}
