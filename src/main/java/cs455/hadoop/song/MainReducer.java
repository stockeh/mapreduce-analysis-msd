package cs455.hadoop.song;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import cs455.hadoop.util.DocumentUtilities;
import cs455.hadoop.util.Song;
import cs455.hadoop.util.SongData;

/**
 * Reducer: Input to the reducer is the output from the mapper. It
 * receives word, list<count> pairs. Sums up individual counts per
 * given word. Emits <word, total count> pairs.
 */
public class MainReducer extends Reducer<Text, Text, Text, DoubleWritable> {

  private static final Map<Text, Song> songs = new HashMap<>();

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

    DocumentUtilities.addSongData( songs, SongData.HOTNESS, songTitle,
        hotness );

    DocumentUtilities.addSongData( songs, SongData.DURATION, songTitle,
        duration );

    DocumentUtilities.addSongData( songs, SongData.DANCE_ENERGY, songTitle,
        dancergy );
  }

  @Override
  protected void cleanup(Context context)
      throws IOException, InterruptedException {

    context.write(
        new Text( "\n----Q3. Song with the highest hotness score----" ),
        new DoubleWritable() );
    top( context, SongData.HOTNESS, true );

    context.write(
        new Text( "\n----Q5. Longest, shortest, and meadian length songs----" ),
        new DoubleWritable() );
    context.write( new Text( "\n----LONGEST----" ), new DoubleWritable() );
    top( context, SongData.DURATION, true );

    context.write( new Text( "\n----SHORTEST----" ), new DoubleWritable() );
    top( context, SongData.DURATION, false );

    context.write( new Text( "\n----AVERAGE----" ), new DoubleWritable() );
    songDurationAverage( context );

    context.write(
        new Text( "\n----Q6. Most energetic and danceabile songs----" ),
        new DoubleWritable() );
    top( context, SongData.DANCE_ENERGY, true );

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
  private void top(Context context, final SongData type, boolean decending)
      throws IOException, InterruptedException {

    final Map<Text, Song> sorted =
        DocumentUtilities.sortMapByValue( songs, type, decending );

    DocumentUtilities.writeMapToContext( context, sorted, type, 10 );
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
    double average = Song.totalDuration / Song.totalSongsOfDuration;
    context.write( new Text( "Actual Average" ),
        new DoubleWritable( average ) );
    final SongData type = SongData.DURATION;
    Map<Text, Song> averageMap = new HashMap<>();
    for ( Entry<Text, Song> entry : songs.entrySet() )
    {
      Song song = entry.getValue();
      if ( type.isInvalid( song ) )
      {
        double value = song.getDuration();
        if ( value < average + 1 && value > average - 1 )
        {
          averageMap.put( entry.getKey(), song );
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
