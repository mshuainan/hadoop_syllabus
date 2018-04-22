package grouping_comparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPartitioner extends Partitioner<OrderBean, NullWritable> {
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numPartitions) {
        int parCode = (orderBean.getOrder_id() & Integer.MAX_VALUE) % numPartitions;
        return parCode;
    }
}
