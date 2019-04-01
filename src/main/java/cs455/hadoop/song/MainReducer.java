package cs455.hadoop.song;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import cs455.hadoop.util.ArtistData;
import cs455.hadoop.util.Data;
import cs455.hadoop.util.DocumentUtilities;
import cs455.hadoop.util.Item;
import cs455.hadoop.util.Song;
import cs455.hadoop.util.SongData;

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

    context.write( new Text( "\n----Q6. MOST ENERGETIC AND DANCEABILE SONGS" ),
        new DoubleWritable() );
    top( context, songs, SongData.DANCE_ENERGY, true );

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
