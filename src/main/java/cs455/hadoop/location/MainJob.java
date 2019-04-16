package cs455.hadoop.location;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This is the main class. Hadoop will invoke the main method of this
 * class.
 * 
 * @author stock
 * 
 */
public class MainJob {

  /**
   * Only a single job is used for this class.
   * 
   * @author stock
   *
   */
  public static class ChainJobs extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
      Configuration conf = new Configuration();
      runJob1( args, conf );
      return runJob2( args, conf );
    }

    /**
     * 
     * @param args
     * @param conf
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    private void runJob1(String[] args, Configuration conf)
        throws IOException, ClassNotFoundException, InterruptedException {

      Job job = Job.getInstance( conf, "1 - Location Analysis" );
      job.setJarByClass( MainJob.class );
      job.setNumReduceTasks( 1 );

      job.setMapOutputKeyClass( Text.class );
      job.setMapOutputValueClass( Text.class );
      job.setOutputKeyClass( Text.class );
      job.setOutputValueClass( Text.class );

      job.setMapperClass( MetadataMap.class );
      job.setReducerClass( CoordinateReducer.class );

      FileInputFormat.setInputPaths( job, new Path( args[ 0 ] ) );
      FileOutputFormat.setOutputPath( job, new Path( args[ 2 ] + "/coords" ) );

      job.waitForCompletion( true );
    }

    /**
     * 
     * @param args
     * @param conf
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    private int runJob2(String[] args, Configuration conf)
        throws IOException, ClassNotFoundException, InterruptedException {

      Job job = Job.getInstance( conf, "2 - Location Analysis" );
      job.setJarByClass( MainJob.class );
      job.setNumReduceTasks( 1 );

      job.setMapOutputKeyClass( Text.class );
      job.setMapOutputValueClass( Text.class );
      job.setOutputKeyClass( Text.class );
      job.setOutputValueClass( NullWritable.class );

      MultipleInputs.addInputPath( job, new Path( args[ 0 ] ),
          TextInputFormat.class, MetadataMap.class );
      MultipleInputs.addInputPath( job, new Path( args[ 1 ] ),
          TextInputFormat.class, AnalysisMap.class );

      job.setReducerClass( MainReducer.class );

      FileOutputFormat.setOutputPath( job, new Path( args[ 2 ] + "/samples" ) );

      return job.waitForCompletion( true ) ? 0 : 1;
    }
  }

  /**
   * Driver method to start job
   * 
   * @param args
   */
  public static void main(String[] args) {
    for ( int i = 0; i < args.length; i++ )
    {
      System.out.println( args[ i ] );
    }
    if ( args.length != 3 )
    {
      System.err.println( "Invalid Argument Configurations - add / remove" );
      System.exit( 0 );
    }
    int status = 0;
    try
    {
      status = ToolRunner.run( new Configuration(), new ChainJobs(), args );
    } catch ( Exception e )
    {
      System.err.println( e.getMessage() );
    }
    System.exit( status );
  }
}
