package cs455.hadoop.items;

import org.apache.hadoop.io.Text;

/**
 * Class representing information regarding a song.
 * 
 * This includes the songs following:
 * 
 * <ul>
 * <li>hotness</li>
 * <li>duration</li>
 * <li>dance & energy</li>
 * </ul>
 * 
 * @author stock
 *
 */
public class Song implements Item {

  private Double hotness = new Double( EPSILON );

  private Double duration = new Double( EPSILON );

  private Double dancergy = new Double( EPSILON );

  private Text name;

  public static double totalDuration = 0;

  public static double totalSongsOfDuration = 0;

  public Song(Text name) {
    this.name = name;
  }

  public Text getName() {
    return this.name;
  }

  /**
   * 
   * @return this songs hotness measure
   */
  public Double getHotness() {
    return this.hotness;
  }

  /**
   * 
   * @return this songs duration in seconds
   */
  public Double getDuration() {
    return this.duration;
  }

  /**
   * 
   * @return this songs dancing energy
   */
  public Double getDancergy() {
    return this.dancergy;
  }

  /**
   * Set the hotness for this song.
   * 
   * @param value
   */
  public void setHotness(double value) {
    this.hotness = new Double( value );
  }

  /**
   * Set the duration, in seconds, for this song. The static total
   * duration is computed for <b>ALL</b> songs, and the total number of
   * songs are incremented. This will assist in computing the global
   * average for all songs.
   * 
   * @param value
   */
  public void setDuration(double value) {
    this.duration = new Double( value );
    Song.totalDuration += value;
    Song.totalSongsOfDuration += 1;
  }

  /**
   * Set the dancing energy for this song.
   * 
   * @param value
   */
  public void setDancergy(double value) {
    this.dancergy = new Double( value );
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return hotness.toString() + "\t" + duration.toString() + "\t"
        + dancergy.toString();
  }
}
