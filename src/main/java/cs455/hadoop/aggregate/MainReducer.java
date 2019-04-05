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
import cs455.hadoop.util.DocumentUtilities.RandomDoubleGenerator;

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

    int count = 0;
    for ( List<Double> arr : ITEMS )
    {
      X[ count++ ] = arr.stream().mapToDouble( Double::doubleValue ).toArray();
    }
    OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();
    regression.newSampleData( T, X );
    double[] a = search( context, regression, X, T );
  }

  private double[] search(Context context,
      OLSMultipleLinearRegression regression, double[][] X, double[] T)
      throws IOException, InterruptedException {

    DocumentUtilities.RandomDoubleGenerator generator =
        new RandomDoubleGenerator( -2, 2 );

    final int nItems = X[ 0 ].length;
    final int nSamples = 10;

    double[][] samples = new double[ nSamples ][ nItems ];

    int loc = 0;
    for ( int i = 0; i < T.length; i++ )
    {
      if ( T[ i ] == 1.0 )
      {
        samples[ loc++ ] = X[ i ];
        context.write(
            new Text(
                "\n----SAMPLE: " + Arrays.toString( samples[ loc - 1 ] ) ),
            new DoubleWritable( T[ i ] ) );
      }
      if ( loc == nSamples )
      {
        break;
      }
    }
    double[] Xn = new double[ nItems ];
    double[] beta = regression.estimateRegressionParameters();
    double hotness = 0;
    boolean done = false;
    while ( !done )
    {
      for ( int s = 0; s < nSamples; ++s )
      {
        for ( int i = 0; i < nItems; ++i )
        {
          Xn[ i ] = samples[ s ][ i ] * generator.nextDouble();
        }
        hotness = use( context, beta, Xn );
        if ( hotness > 1 )
        {
          done = true;
          break;
        }
      }
    }
    context.write( new Text( "\n----WEIGHTS: " + Arrays.toString( beta ) ),
        new DoubleWritable() );

    context.write( new Text( "\n----SAMPLE: " + Arrays.toString( Xn ) ),
        new DoubleWritable( hotness ) );

    // for ( int s = 0; s < nSamples; ++s )
    // {
    // double hotness = use( context, beta, samples[ s ] );
    //
    // context.write( new Text( "\n----SAMPLE: "
    // + Arrays.toString( samples[ s ] ) + ",\tPredicted:" ),
    // new DoubleWritable( hotness ) );
    //
    // }
    return Xn;
  }

  private double use(Context context, double[] beta, double[] X)
      throws IOException, InterruptedException {
    // intercept at beta[0]
    double prediction = beta[ 0 ];
    for ( int i = 1; i < beta.length; i++ )
    {
      prediction += beta[ i ] * X[ i - 1 ];
    }
    return prediction;
  }

}
