package grouping_comparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupingComparator extends WritableComparator{
    public OrderGroupingComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean aBean = (OrderBean)a;
        OrderBean bBean = (OrderBean)b;

        //我们按照价格进行降序排序
        int result;
        if(aBean.getOrder_id() > bBean.getOrder_id()) {
            result = 1;
        }else if(aBean.getOrder_id() < bBean.getOrder_id()){
            result = -1;
        }else{
            result = 0;
        }
        return result;
    }
}
