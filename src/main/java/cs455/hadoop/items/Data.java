package cs455.hadoop.items;

import java.util.Comparator;
import java.util.Map.Entry;
import org.apache.hadoop.io.Text;

/**
 * Interface to make the functions behind a <code>Song</code> and
 * <code>Artist</code>.
 * 
 * @author stock
 *
 */
public interface Data {

  /**
   * Used to compare specific information for a given <code>Item</code>,
   * whether it be a <code>Song</code> or <code>Artist</code>.
   * 
   * This could pertain to any of the elements specific to that class.
   * 
   * @return a comparator to allow correct sorting of values
   */
  public Comparator<Entry<Text, Item>> comparator();

  /**
   * Add a new value to the specified <code>Item</code>. This can
   * include any member specified for the inherited class.
   * 
   * @param item to add too, this could be of type <code>Song</code> or
   *        <code>Artist</code>
   * @param value to add to the inherited member class
   */
  public void add(Item item, double value);

  /**
   * Constraints for a specified <code>Item</code> being valid. Each
   * member of the inherited class could have a different specification
   * for what is meant to be <i>valid</i>.
   * 
   * This is useful for when an item is being written to
   * <code>Context</code>.
   * 
   * @param item
   * @return true, if the value is valid, false otherwise
   */
  public boolean isInvalid(Item item);

  /**
   * Get the member associated with a specified <code>Item</code>.
   * 
   * @param item
   * @return a member of the <code>Item</code> child class
   */
  public Double getValue(Item item);

  /**
   * Used to create a new <code>Song</code> or <code>Artist</code> as
   * specified by the enumerated value.
   * 
   * @return a new <code>Item</code> relating to a <code>Song</code> or
   *         <code>Artist</code> object
   */
  public Item getNewItem(Text name);

}
