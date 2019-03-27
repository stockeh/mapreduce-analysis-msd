package cs455.hadoop.artistmost;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This is the main class. Hadoop will invoke the main method of this
 * class.
 */
public class MainJob {

  public static void main(String[] args) {
    try
    {
      Configuration conf = new Configuration();

      Job job = Job.getInstance( conf, "Artist Most" );
      job.setJarByClass( MainJob.class );

      job.setMapperClass( MainMapper.class );
      job.setCombinerClass( MainReducer.class );
      job.setReducerClass( MainReducer.class );

      job.setMapOutputKeyClass( Text.class );
      job.setMapOutputValueClass( IntWritable.class );
      job.setOutputKeyClass( Text.class );
      job.setOutputValueClass( IntWritable.class );

      FileInputFormat.addInputPath( job, new Path( args[ 0 ] ) );
      FileOutputFormat.setOutputPath( job, new Path( args[ 1 ] ) );

      System.exit( job.waitForCompletion( true ) ? 0 : 1 );
    } catch ( IOException e )
    {
      System.err.println( e.getMessage() );
    } catch ( InterruptedException e )
    {
      System.err.println( e.getMessage() );
    } catch ( ClassNotFoundException e )
    {
      System.err.println( e.getMessage() );
    }

  }
}
