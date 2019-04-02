package cs455.hadoop.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import cs455.hadoop.items.ArtistRank;
import cs455.hadoop.items.Data;
import cs455.hadoop.items.Item;

public class DocumentUtilities {

  /**
   * Split the input document with embedded commas ( , ) in data.
   * 
   * This is equivalent to the regular expression:
   * ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
   * 
   * @param s input to split based on comma
   * @return an <code>ArrayList</code> of the split string
   */
  public static ArrayList<String> splitString(String s) {
    ArrayList<String> words = new ArrayList<String>();

    boolean notInsideComma = true;
    int start = 0;

    for ( int i = 0; i < s.length() - 1; i++ )
    {
      if ( s.charAt( i ) == ',' && notInsideComma )
      {
        words.add( s.substring( start, i ) );
        start = i + 1;
      } else if ( s.charAt( i ) == '"' )
        notInsideComma = !notInsideComma;
    }

    words.add( s.substring( start ) );

    return words;
  }

  /**
   * Sort the map depending on the specified song data type in ascending
   * or descending order.
   * 
   * @param map HashMap of the values
   * @param type type of data
   * @param descending true for descending, false for ascending
   * @return a new map of sorted <K, V> pairs.=
   */
  public static Map<Text, Item> sortMapByValue(Map<Text, Item> map,
      final Data type, boolean descending) {

    Comparator<Entry<Text, Item>> comparator = type.comparator();

    if ( descending )
    {
      comparator = Collections.reverseOrder( comparator );
    }
    return map.entrySet().stream().sorted( comparator )
        .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue,
            (e1, e2) -> e2, LinkedHashMap::new ) );
  }

  /**
   * Create a new <code>Song</code> object for a given song
   * 
   * @param map HashMap containing values
   * @param type type of data
   * @param name association with the specified key
   * @param key HashMap Key
   * @param value HashMap value for specified type
   */
  public static void addData(Map<Text, Item> map, final Data type, Text name,
      Text key, double value) {

    Item item = map.get( key );
    if ( item == null )
    {
      item = type.getNewItem( name );
      map.put( key, item );
    }
    type.add( item, value );
  }

  /**
   * Returns a new double initialized to the value represented by the
   * specified <code>String</code>.
   * 
   * @param str
   * @return returns number if valid, 0 otherwise.
   */
  public static double parseDouble(String str) {
    try
    {
      return Double.parseDouble( str );
    } catch ( NumberFormatException e )
    {
      return 0;
    }
  }

  @SuppressWarnings( { "unchecked", "rawtypes" } )
  public static void writePageRankToContext(Context context,
      Map<String, ArtistRank> map, boolean descending, int numElements)
      throws IOException, InterruptedException {

    Comparator<Entry<String, ArtistRank>> comparator =
        (e1, e2) -> ( ( ArtistRank ) e1.getValue() ).getRank()
            .compareTo( ( ( ArtistRank ) e2.getValue() ).getRank() );

    if ( descending )
    {
      comparator = Collections.reverseOrder( comparator );
    }
    Map<String, ArtistRank> sorted = map.entrySet().stream()
        .sorted( comparator ).collect( Collectors.toMap( Map.Entry::getKey,
            Map.Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new ) );

    int count = 0;
    for ( ArtistRank entry : sorted.values() )
    {
      if ( count++ < numElements )
      {
        context.write( new Text( entry.getArtistName() ),
            new DoubleWritable( entry.getRank() ) );
      } else
      {
        break;
      }
    }
  }

  /**
   * Write out the first <code>numElements</code> of a <code>Map</code>
   * to context.
   * 
   * @param context
   * @param map
   * @param numElements
   * @throws IOException
   * @throws InterruptedException
   */
  @SuppressWarnings( { "rawtypes", "unchecked" } )
  public static void writeMapToContext(Context context, Map<Text, Item> map,
      final Data type, int numElements)
      throws IOException, InterruptedException {

    int count = 0;
    for ( Entry<Text, Item> entry : map.entrySet() )
    {
      Item item = entry.getValue();
      if ( type.isInvalid( item ) )
      {
        if ( count++ < numElements )
        {
          context.write( item.getName(),
              new DoubleWritable( type.getValue( item ) ) );
        } else
        {
          break;
        }
      }
    }
  }
}
