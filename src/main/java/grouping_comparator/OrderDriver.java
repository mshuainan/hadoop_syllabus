package grouping_comparator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class OrderDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "OrderDriver");

        job.setJarByClass(OrderDriver.class);

        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        //设置分区器
        job.setPartitionerClass(OrderPartitioner.class);
        job.setNumReduceTasks(3);

        //设置分组器
        job.setGroupingComparatorClass(OrderGroupingComparator.class);

        FileInputFormat.addInputPath(job, new Path("D:\\test_data\\order_data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\test_data\\output"));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new OrderDriver(), args);
        System.exit(status);
    }
}
