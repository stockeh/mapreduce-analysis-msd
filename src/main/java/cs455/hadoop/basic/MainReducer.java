package cs455.hadoop.basic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
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
import cs455.hadoop.items.ArtistRank;
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

  private static final Map<String, ArtistRank> links = new HashMap<>();

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
    List<String> similarArtistIDs = null;

    for ( Text v : values )
    {
      String[] elements = v.toString().split( "\t" );

      if ( elements.length == 7 )
      {
        loudness = DocumentUtilities.parseDouble( elements[ 0 ] );
        duration = DocumentUtilities.parseDouble( elements[ 4 ] );
        // TODO: Check , duration == 0 ? IDK : duration
        fade = DocumentUtilities.parseDouble( elements[ 1 ] )
            + ( duration - DocumentUtilities.parseDouble( elements[ 2 ] ) );
        hotness = DocumentUtilities.parseDouble( elements[ 3 ] );
        dancergy = DocumentUtilities.parseDouble( elements[ 5 ] )
            * DocumentUtilities.parseDouble( elements[ 6 ] );
      } else
      {
        songTitle = new Text( elements[ 0 ] );
        artistName = new Text( elements[ 1 ] );
        artistID = elements[ 3 ];
        similarArtistIDs =
            new ArrayList<>( Arrays.asList( elements[ 2 ].split( "\\s+" ) ) );
      }
    }

    Text ID = new Text( artistID );
    DocumentUtilities.addData( artists, ArtistData.TOTAL_SONGS, artistName, ID,
        0 );
    DocumentUtilities.addData( artists, ArtistData.LOUDNESS, artistName, ID,
        loudness );
    DocumentUtilities.addData( artists, ArtistData.FADE_DURATION, artistName,
        ID, fade );
    links.put( artistID, new ArtistRank( artistName, similarArtistIDs, 1.0 ) );

    Text songID = new Text( key );
    DocumentUtilities.addData( songs, SongData.HOTNESS, songTitle, songID,
        hotness );
    DocumentUtilities.addData( songs, SongData.DURATION, songTitle, songID,
        duration );
    DocumentUtilities.addData( songs, SongData.DANCE_ENERGY, songTitle, songID,
        dancergy );
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

    context.write(
        new Text( "\n----Q8. MOST GENERIC & UNIQUE ARTIST BY PAGERANK" ),
        new DoubleWritable() );
    pageRank( context );

  }

  /**
   * Compute the modified page rank of artists and list of similar
   * artists. This is done iteratively, by taking the following steps:
   * 
   * <ol>
   * <li>join the links and ranks on artist_id. Ranks with outgoing
   * artist_id's not in the rank keys are ignored.</li>
   * <li>compute the contribution for each outgoing link by assigning
   * the link to associated rank divided by the number of outgoing
   * links. This is similar to reduce by key for each artist over all
   * outgoing link.</li>
   * <li>repeat steps 1. and 2. for each artist.</li>
   * <li>update the ranks based on the contributions for all the
   * artists. Then map the values to update the ranks.</li>
   * <li>repeat steps 1. - 4. for N iterations</li>
   * </ol>
   * 
   * @param context
   * @throws IOException
   * @throws InterruptedException
   */
  private void pageRank(Context context)
      throws IOException, InterruptedException {
    int N = 10;

    while ( N-- > 0 )
    {
      /**
       * Iterate.
       */
      Map<String, Double> contrib = new HashMap<>();
      for ( Entry<String, ArtistRank> entry : links.entrySet() )
      {
        /**
         * Step 1.
         */
        ArtistRank val = entry.getValue();
        List<String> urls = val.getSimilarArtistIDs();

        // Need modified values in case there are outgoing links that are do
        // not exist in the set of keys from <code>links</code>.
        List<String> modified = new ArrayList<>();
        for ( String url : urls )
        {
          if ( links.containsKey( url ) )
          {
            modified.add( url );
          }
        }

        /**
         * Step 2.
         */
        double contribValue = val.getRank() / modified.size();

        for ( String url : modified )
        {
          contrib.put( url, contrib.getOrDefault( url, 0.0 ) + contribValue );
        }
      }
      /**
       * Step 3.
       */
      for ( Entry<String, ArtistRank> entry : links.entrySet() )
      {
        String key = entry.getKey();
        ArtistRank val = entry.getValue();
        val.updateRank( contrib.getOrDefault( key, -0.175 ) );
      }
    }
    context.write( new Text( "\n----GENERIC----" ), new DoubleWritable() );
    writePageRankToContext( context, links, true, 10 );

    context.write( new Text( "\n----UNIQUE----" ), new DoubleWritable() );
    writePageRankToContext( context, links, false, 10 );
  }

  /**
   * Custom writer to sort the links of the page rank results
   * accordingly, then write out the results to context.
   * 
   * @param context
   * @param map
   * @param descending
   * @param numElements
   * @throws IOException
   * @throws InterruptedException
   */
  public static void writePageRankToContext(Context context,
      Map<String, ArtistRank> map, boolean descending, int numElements)
      throws IOException, InterruptedException {

    Comparator<Entry<String, ArtistRank>> comparator =
        (e1, e2) -> ( ( ArtistRank ) e1.getValue() ).getRank()
            .compareTo( ( ( ArtistRank ) e2.getValue() ).getRank() );

    if ( descending )
    {
      comparator = Collections.reverseOrder( comparator );
    }
    Map<String, ArtistRank> sorted = map.entrySet().stream()
        .sorted( comparator ).collect( Collectors.toMap( Map.Entry::getKey,
            Map.Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new ) );

    int count = 0;
    for ( ArtistRank entry : sorted.values() )
    {
      if ( count++ < numElements )
      {
        context.write( new Text( entry.getArtistName() ),
            new DoubleWritable( entry.getRank() ) );
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
   * Find the 10 songs that fall in the median for the corpus.
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
      } else if ( index + 5 > median )
      {
        Item item = entry.getValue();
        if ( type.isInvalid( item ) )
        {
          context.write( item.getName(),
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
   * Find and display the 10 songs surrounding the average song
   * duration.
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
