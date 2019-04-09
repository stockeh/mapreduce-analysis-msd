package cs455.hadoop.aggregate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;
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

  // Statistics for the average segment lengths
  private static IntSummaryStatistics[] AVG_SEG_STATS;

  /**
   * Called once at the beginning of the task.
   */
  @Override
  protected void setup(Context context)
      throws IOException, InterruptedException {
    AVG_SEG_STATS =
        new IntSummaryStatistics[ DocumentUtilities.SEGMENT_KEYS.length ];
    for ( int i = 0; i < AVG_SEG_STATS.length; i++ )
    {
      AVG_SEG_STATS[ i ] = new IntSummaryStatistics();
    }
  }

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

    String terms = null;

    for ( Text v : values )
    {
      String[] elements = v.toString().split( "\t" );
      final int nElements = elements.length;
      if ( nElements == AVG_SEG_STATS.length )
      {
        for ( int i = 0; i < AVG_SEG_STATS.length; i++ )
        {
          AVG_SEG_STATS[ i ]
              .accept( DocumentUtilities.parseInt( elements[ i ] ) );
        }
      } else if ( nElements == ( nFeatures + 1 ) )
      {
        hotness = DocumentUtilities.parseDouble( elements[ 0 ] );
        for ( int i = 1; i < nElements; i++ )
        {
          item.add( DocumentUtilities.parseDouble( elements[ i ] ) );
        }
        ++it;
      } else if ( nElements == 1 )
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

  /**
   * Execute the cleanup <b>once</b> after all items have been reduced.
   */
  @Override
  protected void cleanup(Context context)
      throws IOException, InterruptedException {

    StringBuilder sb = new StringBuilder();

    sb.append( DocumentUtilities.AVG_IDENTIFIER );
    for ( int i = 0; i < AVG_SEG_STATS.length; i++ )
    {
      sb.append( "," ).append( Math.round( AVG_SEG_STATS[ i ].getAverage() ) );
    }
    // AVG_IDENTIFIER,###,###,###,###,###,###
    context.write( new Text( sb.toString() ), NullWritable.get() );

    context.write( new Text( "\n----Q9. SONG WITH A HIGHER HOTNESS" ),
        NullWritable.get() );
    displayNewSample( context, findNewSample( context ) );
  }

  /**
   * Pretty write the results of the best sample to context.
   * 
   * @param context to write too
   * @param newSample found when regressing over all samples
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
    String terms = TERMS.get( newSample.get( last ).intValue() );
    sb.append( labels[ last ] ).append( terms );

    String[] adj = terms.split( "\\s+" );
    String title = "\nThe Next "
        + StringUtils.camelize(
            adj[ ( int ) ( System.currentTimeMillis() % adj.length ) ] )
        + " Banger, By Stock Studios";
    context.write( new Text( title ), NullWritable.get() );
    context.write( new Text( sb.toString() ), NullWritable.get() );
  }

  /**
   * Find the <i>best</i> new sample that has a hotness score greater
   * than the highest in the data.
   * 
   * This method introduces use of multiple linear regression to fit a
   * hyper plane to the sample space. The function
   * <code>g(x<sub>n</sub>; w)</code> is parameterized by the vector
   * <code>w</code>; and using ordinary least squares, we can
   * approximate the values of <code>w</code> to fit a model.
   * 
   * <pre>
   * <code> X<sup>T</sup> T = X<sup>T</sup> X w </code>
   * </pre>
   * 
   * This approximates a solution for that minimizes the squared error
   * for all <code>X</code>.
   * 
   * @param context used to write out the solution of the parameters
   *        <code>w</code>
   * @return a <code>List</code> of features for the new sample that has
   *         a hotness value <code>T<sub>n</sub></code> greater than the
   *         maximum in the data, <code>T</code>
   * @throws IOException
   * @throws InterruptedException
   */
  private List<Double> findNewSample(Context context)
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

    final int nSamples = 10;

    double[] indices = new double[ nSamples ];

    int loc = 0;
    for ( int i = 0; i < T.length; i++ )
    {
      if ( T[ i ] == 1.0 )
      {
        indices[ loc++ ] = i;
      }
      if ( loc == nSamples )
      {
        break;
      }
    }
    // Model Parameters, w_0, w_1, ..., w_n
    final double[] beta = regression.estimateRegressionParameters();

    context.write( new Text( "\nWeights: " + Arrays.toString( beta ) ),
        NullWritable.get() );

    return search( context, X, indices, beta );
  }


  /**
   * Perform a search in the parameter space of <code>w</code> to find a
   * solution of <code>X</code> which results in a hotness target
   * <code>T</code> greater than the maximum in the data.
   * 
   * This is done by looping through each sample index, getting the
   * feature values of <code>X<sub>index</sub></code>, randomly altering
   * each feature values and computing an approximate hotness value
   * <code>T<sub>n</sub></code>. If this is greater than the maximum in
   * <code>T</code>, the modified feature values are the new greatest.
   * 
   * @param X the <code>N x D</code> matrix <code>X</code> with all
   *        <code>D</code> features for <code>N</code> samples
   * @param indices index values relating to the subset of samples with
   *        <code>T = 1</code> (the maximum). This will assist in
   *        retrieving the terms associated with the original sample
   * @param beta parameter values <code>w</code> that will be used to
   *        solve for <code>T<sub>n</sub></code>
   * @return a <code>List</code> of features for the new sample that has
   *         a hotness value <code>T<sub>n</sub></code> greater than the
   *         maximum in the data <code>T</code>
   * @throws InterruptedException
   * @throws IOException
   */
  private List<Double> search(Context context, double[][] X, double[] indices,
      double[] beta) throws IOException, InterruptedException {

    // TODO: Lower these values with true dataset
    RandomDoubleGenerator generator = new RandomDoubleGenerator( 0.7, 3.0 );

    double[] Xn = new double[ nFeatures ];

    double hotness = 0;
    int index = 0;
    double featureVal = 0;

    boolean done = false;
    while ( !done )
    {
      for ( int s = 0; s < indices.length; ++s )
      {
        index = ( int ) indices[ s ];
        for ( int i = 0; i < nFeatures; ++i )
        {
          featureVal = X[ index ][ i ] * generator.nextDouble();
          if ( i == 2 || i == 4 || i == 7 )
          { // These are integer values
            featureVal = Math.round( featureVal );
          }
          Xn[ i ] = featureVal;
        }
        hotness = use( beta, Xn );
        if ( hotness > 1 )
        {
          done = true;
          break;
        }
      }
    }
    List<Double> newSample =
        DoubleStream.of( Xn ).boxed().collect( Collectors.toList() );
    newSample.add( 0, hotness );
    newSample.add( 1, 0.0 ); // danceability
    newSample.add( 4, 0.0 ); // energy
    newSample.add( ( double ) index ); // index of terms from sample

    return newSample;
  }

  /**
   * Compute the function to approximate a hotness value
   * <code>T<sub>n</sub></code>:
   * 
   * <pre>
   * <code>
   * g(x<sub>n</sub>; w) = w<sub>0</sub> + w<sub>1</sub>x<sub>1</sub> + ... + w<sub>D</sub>x<sub>D</sub>
   * </code>
   * </pre>
   * 
   * @param beta parameter values <code>w</code> that will be used to
   *        solve for <code>T<sub>n</sub></code>
   * @param Xn supplement values for a sample <code>X<sub>n</sub></code>
   * @return a prediction for <code>g(x<sub>n</sub>; w)</code>
   */
  private double use(double[] beta, double[] Xn) {
    // intercept at beta[0]
    double prediction = beta[ 0 ];
    for ( int i = 1; i < beta.length; i++ )
    {
      prediction += beta[ i ] * Xn[ i - 1 ];
    }
    return prediction;
  }

}
