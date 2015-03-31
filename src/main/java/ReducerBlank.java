import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by yaroslav on 3/31/15.
 *
 * @param <Text>           input key   :
 * @param <IntWritable>    input value :
 * @param <Text>           output key  :
 * @param <IntWritable>    output value:
 */
public class ReducerBlank extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    private IntWritable amount = new IntWritable(0);

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        
        int sum = 0;
        
        for(IntWritable value: values) {
            sum += value.get();
        }
        
        amount.set(sum);
        context.write(key, amount);
    }
}
