package comparator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FlowSortDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "FlowSortDriver");

        job.setJarByClass(FlowSortDriver.class);

        job.setMapperClass(FlowSortMapper.class);
        job.setReducerClass(FlowSortReducer.class);

        job.setMapOutputKeyClass(FlowSortBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowSortBean.class);

        job.setPartitionerClass(ProvinceSortPartitioner.class);
        job.setNumReduceTasks(5);

        FileInputFormat.addInputPath(job, new Path("D:\\test_data\\phone_data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\test_data\\output"));



        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new FlowSortDriver(), args);
        System.exit(status);
    }
}
