package cs455.hadoop.aggregate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.apache.hadoop.io.NullWritable;
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
public class MainReducer extends Reducer<Text, Text, Text, NullWritable> {

  // N x 1 matrix of N target values
  public static final List<Double> HOT = new ArrayList<>();

  // N x D matrix of N samples with D features
  public static final List<List<Double>> ITEMS = new ArrayList<>();

  public static final List<String> TERMS = new ArrayList<>();

  // D number of features in each sample
  public static final int nFeatures = 8;

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
    String terms = null;

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
      } else if ( elements.length == 1 )
      {
        terms = elements[ 0 ];
        ++it;
      }
    }
    if ( it == 2 && item.size() == nFeatures )
    {
      HOT.add( hotness );
      ITEMS.add( item );
      TERMS.add( terms );
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

    List<Double> newSample = findNewSample( context, regression, X, T );
    displayNewSample( context, newSample );
  }

  /**
   * 
   * @param context
   * @param newSample
   * @throws IOException
   * @throws InterruptedException
   */
  private void displayNewSample(Context context, List<Double> newSample)
      throws IOException, InterruptedException {

    String[] labels = new String[] { "Hotness: ", ", Danceability: ",
        ", Duration: ", ", End Fade In: ", ", Energy: ", ", Key: ",
        ", Loudness: ", ", Mode: ", ", Start Fade Out: ", ", Tempo: ",
        ", Time Signature: ", "\nTerms: " };

    StringBuilder sb = new StringBuilder();
    int last = labels.length - 1;
    for ( int i = 0; i < last; i++ )
    {
      sb.append( labels[ i ] )
          .append( String.format( "%.3f", newSample.get( i ) ) );
    }
    sb.append( labels[ last ] )
        .append( TERMS.get( newSample.get( last ).intValue() ) );

    context.write( new Text( "\nThe Next Banger, By Stock Studios" ),
        NullWritable.get() );
    context.write( new Text( sb.toString() ), NullWritable.get() );
  }

  /**
   * 
   * @param context
   * @param regression
   * @param X
   * @param T
   * @return
   * @throws IOException
   * @throws InterruptedException
   */
  private List<Double> findNewSample(Context context,
      OLSMultipleLinearRegression regression, double[][] X, double[] T)
      throws IOException, InterruptedException {
    final int nSamples = 10;

    double[][] samples = new double[ nSamples ][ nFeatures ];
    double[] sampleIndicies = new double[ nSamples ];

    int loc = 0;
    for ( int i = 0; i < T.length; i++ )
    {
      if ( T[ i ] == 1.0 )
      {
        sampleIndicies[ loc ] = i;
        samples[ loc++ ] = X[ i ];
      }
      if ( loc == nSamples )
      {
        break;
      }
    }
    // Model Parameters, W0, W1, ..., Wn
    final double[] beta = regression.estimateRegressionParameters();

    context.write( new Text( "\n----WEIGHTS: " + Arrays.toString( beta ) ),
        NullWritable.get() );

    return search( samples, sampleIndicies, beta );
  }

  /**
   * 
   * 
   * @param context
   * @param regression
   * @param X
   * @param T
   * @return
   * @throws IOException
   * @throws InterruptedException
   */
  private List<Double> search(double[][] samples, double[] sampleIndicies,
      double[] beta) {

    RandomDoubleGenerator generator = new RandomDoubleGenerator( -2, 2 );

    double[] Xn = new double[ nFeatures ];
    double hotness = 0;
    double termsIndex = 0;

    boolean done = false;
    while ( !done )
    {
      for ( int s = 0; s < samples.length; ++s )
      {
        for ( int i = 0; i < nFeatures; ++i )
        {
          Xn[ i ] = samples[ s ][ i ] * generator.nextDouble();
        }
        hotness = use( beta, Xn );
        if ( hotness > 1 )
        {
          termsIndex = sampleIndicies[ s ];
          done = true;
          break;
        }
      }
    }
    List<Double> newSample =
        DoubleStream.of( Xn ).boxed().collect( Collectors.toList() );
    newSample.add( 0, hotness );
    newSample.add( 1, 0.0 );
    newSample.add( 4, 0.0 );
    newSample.add( termsIndex );

    return newSample;
  }

  /**
   * 
   * @param beta
   * @param X
   * @return
   */
  private double use(double[] beta, double[] X) {
    // intercept at beta[0]
    double prediction = beta[ 0 ];
    for ( int i = 1; i < beta.length; i++ )
    {
      prediction += beta[ i ] * X[ i - 1 ];
    }
    return prediction;
  }

}
