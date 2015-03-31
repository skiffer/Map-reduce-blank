import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by yaroslav on 3/26/15.
 */
public class MapReduceDriverBlank extends Configured implements Tool {
    
    
    public static void main(String[] args) throws Exception {
        // Let ToolRunner handle generic command-line options on level of the application
        int res = ToolRunner.run(new Configuration(), new MapReduceDriverBlank(), args);
        
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // Configuration processed by ToolRunner, merged setting of user's and cluster's
        Configuration conf = getConf();

        // Path to directory of data for processing
        // If it runs on cluster, then it will be hdfs paths.
        // If is runs on local file system then it will be local file system paths
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);

        // Create a job and set up general properties
        Job job = Job.getInstance(conf);
        
        // What jar to deploy on the all nodes
        job.setJarByClass(MapReduceDriverBlank.class);
        job.setJobName(getClass().getName());
        
        // Set up input/output paths for the job
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        
        // Set up formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        job.setMapperClass(MapperBlank.class);
        // Optional, depends on a case
//        job.setPartitionerClass();
//        job.setCombinerClass(ReducerBlank.class);
        job.setReducerClass(ReducerBlank.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Submit the job, then poll for progress until the job is complete
        // Synchronize the call, will be in this program until it is running
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
