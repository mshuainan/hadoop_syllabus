package partitioner;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FlowCountRunner extends Configured implements Tool{
    @Override
    public int run(String[] args) throws Exception {
        //组装Job
        Job job = Job.getInstance(getConf(), "自定义Value-FlowBean");

        job.setJarByClass(FlowCountRunner.class);

        job.setMapperClass(FlowCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setReducerClass(FlowCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //设置分区规则，以及reducetask的个数
        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(5);

        FileInputFormat.addInputPath(job, new Path("G:\\bigData\\testFiles\\phone_data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("G:\\bigData\\testFiles\\output"));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new FlowCountRunner(), args);
        System.exit(status);
    }

}
