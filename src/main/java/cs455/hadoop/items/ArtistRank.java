package cs455.hadoop.items;

import java.util.List;
import org.apache.hadoop.io.Text;

/**
 * Class to hold information for computing PageRank for a given
 * artist.
 * 
 * This includes the following:
 * 
 * <ul>
 * <li>Artists Name</li>
 * <li>Similar Artists ID's</li>
 * <li>Rank for this Artist</li>
 * </ul>
 * 
 * @author stock
 *
 */
public class ArtistRank {

  private Text artistName;

  private List<String> similarArtistIDs;

  private double rank;

  /**
   * Default constructor for this class. Populates the member variables.
   * 
   * @param artistName
   * @param similarArtistIDs
   * @param rank
   */
  public ArtistRank(Text artistName, List<String> similarArtistIDs,
      double rank) {
    this.artistName = artistName;
    this.similarArtistIDs = similarArtistIDs;
    this.rank = rank;
  }

  /**
   * 
   * @return the actual name of the artist
   */
  public Text getArtistName() {
    return this.artistName;
  }

  /**
   * 
   * @return a list of ID's similar to this artist
   */
  public List<String> getSimilarArtistIDs() {
    return this.similarArtistIDs;
  }

  /**
   * 
   * @return the current rank of this artist
   */
  public Double getRank() {
    return this.rank;
  }

  /**
   * By default the taxation for this computation is by updating the
   * rank as a factor of 0.85 + 0.15.
   * 
   * @param newRank
   */
  public void updateRank(double contrib) {
    this.rank = 0.15 + 0.85 * contrib;
  }

}
