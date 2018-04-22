package comparator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvinceSortPartitioner extends Partitioner<FlowSortBean, Text> {

    @Override
    public int getPartition(FlowSortBean flowSortBean, Text value, int numPartitions) {
        String phoneNum = value.toString().substring(0, 3);
        int partition;
        switch (phoneNum) {
            case "136":
                partition = 0;
                break;
            case "137":
                partition = 1;
                break;
            case "138":
                partition = 2;
                break;
            case "139":
                partition = 3;
                break;
            default:
                partition = 4;
                break;
        }
        return partition;
    }
}
