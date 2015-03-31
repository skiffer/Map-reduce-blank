import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by yaroslav on 3/31/15.
 *
 * <b>Second version of map-reduce package!</b>
 * 
 * @param <LongWritable>    input key   : Offset in a file
 * @param <Text>            input value : A line from a file
 * @param <Text>            output key  : Word
 * @param <IntWritable>    output value: Count of words
 */
public class MapperBlank extends Mapper<LongWritable, Text, Text, IntWritable> {
    // Use Flyweight pattern for saving memory and GC time
    private final IntWritable one = new IntWritable(1);
    private final Text word = new Text();
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        StringTokenizer itr = new StringTokenizer(value.toString());
        while(itr.hasMoreTokens()) {
            
            word.set(itr.nextToken());
            context.write(word, one);
        
        }
        
    }
}
