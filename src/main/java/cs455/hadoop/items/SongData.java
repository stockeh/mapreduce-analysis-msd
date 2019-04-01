package cs455.hadoop.items;

import java.util.Comparator;
import java.util.Map.Entry;
import org.apache.hadoop.io.Text;

/**
 * Enumerator values, and implemented methods, specific to a
 * <code>Artist</code> object members.
 * 
 * @author stock
 *
 */
public enum SongData implements Data {

  /**
   * Relating to the <i>hotness</i> of a given song.
   */
  HOTNESS
  {
    /**
     * {@inheritDoc}
     */
    @Override
    public Comparator<Entry<Text, Item>> comparator() {
      return (e1, e2) -> ( ( Song ) e1.getValue() ).getHotness()
          .compareTo( ( ( Song ) e2.getValue() ).getHotness() );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void add(Item item, double value) {
      if ( value != 0 )
      {
        ( ( Song ) item ).setHotness( value );
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isInvalid(Item item) {
      return ( ( Song ) item ).getHotness() != cs455.hadoop.items.Item.EPSILON;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Double getValue(Item item) {
      return ( ( Song ) item ).getHotness();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Item getNewItem() {
      return new Song();
    }
  },
  /**
   * Relates to the duration for a given song.
   */
  DURATION
  {
    /**
     * {@inheritDoc}
     */
    @Override
    public Comparator<Entry<Text, Item>> comparator() {
      return (e1, e2) -> ( ( Song ) e1.getValue() ).getDuration()
          .compareTo( ( ( Song ) e2.getValue() ).getDuration() );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void add(Item item, double value) {
      if ( value != 0 )
      {
        ( ( Song ) item ).setDuration( value );
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isInvalid(Item item) {
      return ( ( Song ) item ).getDuration() != cs455.hadoop.items.Item.EPSILON;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Double getValue(Item item) {
      return ( ( Song ) item ).getDuration();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Item getNewItem() {
      return new Song();
    }
  },
  /**
   * Relates to the dance <b>and</b> energy for a given song.
   */
  DANCE_ENERGY
  {
    /**
     * {@inheritDoc}
     */
    @Override
    public Comparator<Entry<Text, Item>> comparator() {
      return (e1, e2) -> ( ( Song ) e1.getValue() ).getDancergy()
          .compareTo( ( ( Song ) e2.getValue() ).getDancergy() );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void add(Item item, double value) {
      if ( value != 0 )
      {
        ( ( Song ) item ).setDancergy( value );
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isInvalid(Item item) {
      return ( ( Song ) item ).getDancergy() != cs455.hadoop.items.Item.EPSILON;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Double getValue(Item item) {
      return ( ( Song ) item ).getDancergy();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Item getNewItem() {
      return new Song();
    }
  };
}
