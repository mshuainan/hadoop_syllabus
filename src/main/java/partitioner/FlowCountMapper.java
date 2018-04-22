package partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    FlowBean bean = new FlowBean();
    Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行数据
        String line = value.toString();

        //切割字段
        String[] fields = line.split("\t");

        //封装对象
        String phoneNum = fields[1];
        //取出上行和下行流量
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);

        bean.setUpFlow(upFlow);
        bean.setDownFlow(downFlow);
        v.set(phoneNum);

        //输出
        context.write(v, bean);
    }
}
