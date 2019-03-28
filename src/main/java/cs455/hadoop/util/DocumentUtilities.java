package cs455.hadoop.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
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
   * Sort a map by value in descending order.
   * 
   * @param map
   * @return a new map of sorted <K, V> pairs.
   */
  public static Map<Text, Double> sortMapByValue(Map<Text, Double> map) {
    return map.entrySet().stream()
        .sorted( Map.Entry.comparingByValue( Comparator.reverseOrder() ) )
        .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue,
            (e1, e2) -> e1, LinkedHashMap::new ) );
  }

  /**
   * Write out the first <code>numElements</code> to context.
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
    for ( Text key : map.keySet() )
    {
      if ( count++ < numElements )
        context.write( key, new DoubleWritable( map.get( key ) ) );
      else
        break;
    }
  }

}
