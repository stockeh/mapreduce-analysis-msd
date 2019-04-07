package cs455.hadoop.segment;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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
      return runJob1( args, conf );
    }

    /**
     * Merge the analysis and metadata to write a file as:
     * 
     * < artist_name, top avg. loudness >
     * 
     * @param args
     * @param conf
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    private int runJob1(String[] args, Configuration conf)
        throws IOException, ClassNotFoundException, InterruptedException {

      Job job = Job.getInstance( conf, "Segment Analysis - Job 1" );
      job.setJarByClass( MainJob.class );
      job.setNumReduceTasks( 6 );

      job.setMapOutputKeyClass( Text.class );
      job.setMapOutputValueClass( Text.class );
      job.setOutputKeyClass( Text.class );
      job.setOutputValueClass( Text.class );

      job.setMapperClass(AnalysisMap.class);

      job.setReducerClass( MainReducer.class );
      
      FileInputFormat.setInputPaths(job, new Path(args[0]));
      FileOutputFormat.setOutputPath( job, new Path( args[ 1 ] ) );

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
    if ( args.length != 2 )
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
