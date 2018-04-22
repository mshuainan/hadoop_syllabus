package mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 <"abc", 1>
 <"aaa", 1>
 <"bbb", 1>
 <"aaa", 1>

 <"123", 1>
 <"aaa", 1>
 <"123", 1>

 -->

 <"abc", [1]>
 <"aaa", [1, 1, 1]>
 <"bbb", [1]>
 <"123", [1, 1]>

 --->

 <"abc", 1>
 <"aaa", 3>
 <"bbb", 1>
 <"123", 2>

 */
public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
    private LongWritable result = new LongWritable();
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        // <"aaa", [1, 1, 1]>
        long count = 0;
        for(LongWritable v: values){//[1, 1, 1]
            count += v.get();
        }
        result.set(count);
        context.write(key, result);
    }
}
