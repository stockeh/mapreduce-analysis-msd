package cs455.hadoop.items;

public interface Item {

  /**
   * Constant value for invalid items. This is used to allow a
   * <code>Song</code> or <code>Artist</code> have various members, even
   * if some are <i>invalid</i>.
   * 
   * When writing a member to <code>Context</code>, this is used to
   * check if valid. A constant of 0 is not used as it is possible the
   * data contains 0, and if desired, will be used ( in the data ).
   */
  public static final double EPSILON = 1e-5;

  /**
   * Implemented method for converting this class to a readable String.
   */
  public String toString();

}
