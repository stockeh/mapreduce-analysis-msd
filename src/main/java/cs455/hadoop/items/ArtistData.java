package cs455.hadoop.items;

import java.util.Comparator;
import java.util.Map.Entry;
import org.apache.hadoop.io.Text;

/**
 * Enumerator values, and implemented methods, specific to a
 * <code>Song</code> object members.
 * 
 * @author stock
 *
 */
public enum ArtistData implements Data {
  /**
   * Relating to the total number of songs for some artist.
   */
  TOTAL_SONGS
  {
    /**
     * {@inheritDoc}
     */
    @Override
    public Comparator<Entry<Text, Item>> comparator() {
      return (e1, e2) -> ( ( Artist ) e1.getValue() ).getTotalSongs()
          .compareTo( ( ( Artist ) e2.getValue() ).getTotalSongs() );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void add(Item item, double value) {
      ( ( Artist ) item ).incrementTotalSongs();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isInvalid(Item item) {
      return ( ( Artist ) item )
          .getTotalSongs() != cs455.hadoop.items.Item.EPSILON;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Double getValue(Item item) {
      return ( ( Artist ) item ).getTotalSongs();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Item getNewItem(Text name) {
      return new Artist(name);
    }
  },
  /**
   * Relating to the average loudness of songs for a given artist.
   */
  LOUDNESS
  {
    /**
     * {@inheritDoc}
     */
    @Override
    public Comparator<Entry<Text, Item>> comparator() {
      return (e1, e2) -> ( ( Artist ) e1.getValue() ).getLoudness()
          .compareTo( ( ( Artist ) e2.getValue() ).getLoudness() );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void add(Item item, double value) {
      if ( value != 0 )
      {
        ( ( Artist ) item ).setLoudness( value );
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isInvalid(Item item) {
      return ( ( Artist ) item )
          .getLoudness() != cs455.hadoop.items.Item.EPSILON;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Double getValue(Item item) {
      return ( ( Artist ) item ).getLoudness();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Item getNewItem(Text name) {
      return new Artist(name);
    }
  },
  /**
   * Relating to the total time spent fading in songs.
   */
  FADE_DURATION
  {
    /**
     * {@inheritDoc}
     */
    @Override
    public Comparator<Entry<Text, Item>> comparator() {
      return (e1, e2) -> ( ( Artist ) e1.getValue() ).getFade()
          .compareTo( ( ( Artist ) e2.getValue() ).getFade() );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void add(Item item, double value) {
      // TODO: CHECK if 0 should be a valid fade in duration.
      if ( value != 0 )
      {
        ( ( Artist ) item ).setFade( value );
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isInvalid(Item item) {
      return ( ( Artist ) item ).getFade() != cs455.hadoop.items.Item.EPSILON;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Double getValue(Item item) {
      return ( ( Artist ) item ).getFade();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Item getNewItem(Text name) {
      return new Artist(name);
    }
  };

}
