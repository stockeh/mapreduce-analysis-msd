package cs455.hadoop.aggregate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import cs455.hadoop.util.DocumentUtilities;

/**
 * Reducer class that takes the output from the mapper and organizes
 * the values accordingly.
 * 
 * @author stock
 *
 */
public class MainReducer extends Reducer<Text, Text, Text, DoubleWritable> {

  public static final List<Double> HOT = new ArrayList<>();

  public static final List<List<Double>> ITEMS = new ArrayList<>();

  /**
   * The mappers use the data's <b>song_id</b> to join the data as a
   * <code>Text key</code>. The values are arranged in no particular
   * order, but differ in split size. Values from the
   * <code>AnalysisMap</code> class have 9 elements shown below. Data
   * from the <code>MetadataMap</code> only has 2 elements. This is used
   * to organize the values to their associated classes.
   */
  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    int it = 0;

    List<Double> item = new ArrayList<>();
    double hotness = 0;

    @SuppressWarnings( "unused" )
    String[] termFrequency = null;
    @SuppressWarnings( "unused" )
    String[] termWeight = null;

    for ( Text v : values )
    {
      String[] elements = v.toString().split( "\t" );

      if ( elements.length == 9 )
      {
        hotness = DocumentUtilities.parseDouble( elements[ 0 ] );
        for ( int i = 1; i < elements.length; i++ )
        {
          item.add( DocumentUtilities.parseDouble( elements[ i ] ) );
        }
        ++it;
      } else
      {
        termFrequency = elements[ 0 ].trim().split( "\\s+" );
        termWeight = elements[ 1 ].trim().split( "\\s+" );
        ++it;
      }
    }
    if ( it == 2 && item.size() == 8 )
    {
      HOT.add( hotness );
      ITEMS.add( item );
    }
  }

  @Override
  protected void cleanup(Context context)
      throws IOException, InterruptedException {

    double[][] X = new double[ ITEMS.size() ][];
    double[] T = HOT.stream().mapToDouble( Double::doubleValue ).toArray();

    int i = 0;
    for ( List<Double> arr : ITEMS )
    {
      X[ i++ ] = arr.stream().mapToDouble( Double::doubleValue ).toArray();
    }

    OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();

    regression.newSampleData( T, X );

    context.write( new Text( "\n---- len: " + X.length + " " + T.length ),
        new DoubleWritable() );

    // for ( int m = 0; m < T.length; m++ )
    // {
    // context.write( new Text( "\n----: " + Arrays.toString( X[ m ] ) ),
    // new DoubleWritable( T[ m ] ) );
    // }

    double[] Xn = X[ 4 ];
    double Tn = T[ 4 ];

    double Yn = use( context, regression, Xn );

    context.write( new Text( "\n---- " + Arrays.toString( Xn ) + ",\tActual: "
        + Tn + ",\tPredicted:" ), new DoubleWritable( Yn ) );
  }

  public double use(Context context, OLSMultipleLinearRegression regression,
      double[] X) throws IOException, InterruptedException {
    if ( regression == null )
    {
      throw new IllegalArgumentException(
          "regression is not intialized, null." );
    }
    double[] beta = regression.estimateRegressionParameters();
    context.write( new Text( "\n---- " + Arrays.toString( beta ) ),
        new DoubleWritable() );

    // intercept at beta[0]
    double prediction = beta[ 0 ];
    for ( int i = 1; i < beta.length; i++ )
    {
      prediction += beta[ i ] * X[ i - 1 ];
    }
    return prediction;
  }

}
