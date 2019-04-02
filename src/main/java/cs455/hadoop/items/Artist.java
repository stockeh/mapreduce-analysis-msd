package cs455.hadoop.items;

import org.apache.hadoop.io.Text;

/**
 * Class representing information regarding an artist.
 * 
 * This includes the songs following:
 * 
 * <ul>
 * <li>average loudness</li>
 * <li>total time spent fading in songs</li>
 * <li>total number of songs</li>
 * </ul>
 * 
 * @author stock
 *
 */
public class Artist implements Item {

  /**
   * Array containing the average [ size of total , running average ]
   */
  private Double[] loudness = new Double[] { 0.0, EPSILON };

  private Double fade = new Double( EPSILON );

  private Double totalSongs = new Double( 0 );

  private Text name;

  public Artist(Text name)
  {
    this.name = name;
  }
  
  public Text getName() {
    return this.name;
  }
  
  /**
   *
   * @return the average loudness for this artist
   */
  public Double getLoudness() {
    return this.loudness[ 1 ];
  }

  /**
   * 
   * @return this artist total time spent fading in songs
   */
  public Double getFade() {
    return this.fade;
  }

  /**
   * 
   * @return this artists total number of songs
   */
  public Double getTotalSongs() {
    return this.totalSongs;
  }

  /**
   * Running average for current artist. This can be computed
   * mathematically by calculating the following:
   * 
   * (size * average + value) / (size + 1);
   * 
   * @param value the running value to add to the average
   */
  public void setLoudness(double value) {
    this.loudness[ 1 ] = ( this.loudness[ 0 ] * this.loudness[ 1 ] + value )
        / ( this.loudness[ 0 ] + 1 );
    this.loudness[ 0 ] += 1;
  }

  /**
   * Add the value for some song to the artists total time spent fading
   * in songs.
   * 
   * @param value to add to the running total of time fading in songs
   */
  public void setFade(double value) {
    this.fade += value;
  }

  /**
   * Increment the total number of songs for this artist.
   */
  public void incrementTotalSongs() {
    this.totalSongs += 1;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return loudness.toString() + "\t" + fade.toString();
  }
}
