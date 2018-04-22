package comparator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowSortMapper extends Mapper<LongWritable, Text, FlowSortBean, Text>{
    FlowSortBean k = new FlowSortBean();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");//以\t分割字段的文件属于.tsv文件，以逗号分割字段的文件属于.csv

        String phoneNum = fields[1];

        long upFlow = Long.valueOf(fields[fields.length - 3]);
        long downFlow = Long.valueOf(fields[fields.length - 2]);

        k.setDownFlow(downFlow);
        k.setUpFlow(upFlow);

        v.set(phoneNum);

        context.write(k, v);
    }
}
