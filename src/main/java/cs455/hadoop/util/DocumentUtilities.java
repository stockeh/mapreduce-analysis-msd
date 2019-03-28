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
   * Sort a map by value in specified order.
   * 
   * @param map
   * @param descending true for descending, false for ascending
   * @return a new map of sorted <K, V> pairs.
   */
  public static Map<Text, Double> sortMapByValue(Map<Text, Double> map,
      boolean descending) {
    Comparator<Map.Entry<Text, Double>> comparator =
        Map.Entry.comparingByValue();
    if ( descending )
    {
      comparator = Collections.reverseOrder( comparator );
    }
    return map.entrySet().stream().sorted( comparator )
        .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue,
            (e1, e2) -> e2, LinkedHashMap::new ) );
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
  public static void writeMapToContext(Context context, Map<Text, Double> map,
      int numElements) throws IOException, InterruptedException {

    int count = 0;
    for ( Entry<Text, Double> entry : map.entrySet() )
    {
      if ( count++ < numElements )
      {
        context.write( entry.getKey(), new DoubleWritable( entry.getValue() ) );
      } else
      {
        break;
      }
    }
  }

}
