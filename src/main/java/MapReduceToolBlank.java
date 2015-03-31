import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
public class MapReduceToolBlank extends Configured implements Tool {
    
    public static void main(String[] args) throws Exception {
        
        // Let ToolRunner handle generic command-line options
        int res = ToolRunner.run(new Configuration(), new MapReduceToolBlank(), args);
        
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // Configuration processed by ToolRunner
        Configuration conf = getConf();

        // Process custom command-line options
        Path in = new Path(args[1]);
        Path out = new Path(args[2]);

        // Create a job and set up general properties
        Job job = Job.getInstance(conf);
        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());
        
        // Set up input/output for the job
        FileInputFormat.addInputPath(job, in);
        job.setInputFormatClass(TextInputFormat.class);

        FileOutputFormat.setOutputPath(job, out);
        job.setOutputFormatClass(TextOutputFormat.class);
        

        // Submit the job, then poll for progress until the job is complete
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
