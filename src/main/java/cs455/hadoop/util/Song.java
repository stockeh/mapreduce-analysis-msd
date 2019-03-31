package cs455.hadoop.util;

/**
 * 
 * 
 * @author stock
 *
 */
public class Song {

  private Double hotness = new Double( EPSILON );

  private Double duration = new Double( EPSILON );

  private Double dancergy = new Double( EPSILON );

  public static double totalDuration = 0;

  public static double totalSongsOfDuration = 0;

  public static final double EPSILON = 1e-5;

  public Double getHotness() {
    return this.hotness;
  }

  public Double getDuration() {
    return this.duration;
  }

  public Double getDancergy() {
    return this.dancergy;
  }

  public void setHotness(double value) {
    this.hotness = new Double( value );
  }

  public void setDuration(double value) {
    this.duration = new Double( value );
    Song.totalDuration += value;
    Song.totalSongsOfDuration += 1;
  }

  public void setDancergy(double value) {
    this.dancergy = new Double( value );
  }

  public String toString() {
    return hotness.toString() + "\t" + duration.toString() + "\t"
        + dancergy.toString();
  }
}
