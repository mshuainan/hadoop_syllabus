package mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
    abc aaa bbb aaa
    123 aaa 123

    <0, "abc aaa bbb aaa">
    <1, "123 aaa 123">

    <"abc", 1>
    <"aaa", 1>
    <"bbb", 1>
    <"aaa", 1>

    <"123", 1>  <Text, LongWritable>
    <"aaa", 1>
    <"123", 1>
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
    private Text outKey = new Text();
    private LongWritable outValue = new LongWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //以第一行数据为例
        String line = value.toString();//"abc aaa bbb aaa"
        String[] words = line.split(" ");//["abc", "aaa", "bbb", "aaa"]
        for(String wd : words){
            outKey.set(wd);//Text("abc")
            context.write(outKey, outValue);//这里会牵扯到序列化
        }
    }
}
