package grouping_comparator;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean>{

    //订单id
    private int order_id;
    //某一个商品的价格
    private double price;

    public OrderBean() {
        super();
    }

    public OrderBean(int order_id, double price) {
        super();
        this.order_id = order_id;
        this.price = price;
    }

    @Override
    public int compareTo(OrderBean o) {
        //我们按照价格进行降序排序
        int result;
        if(order_id > o.getOrder_id()) {
            result = 1;
        }else if(order_id < o.getOrder_id()){
            result = -1;
        }else{
            result = price > o.getPrice() ? -1 : 1;
        }
        //SELECT order_id, price FROM tb_order ORDER BY order_id, price desc;
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(order_id);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.order_id = in.readInt();
        this.price = in.readDouble();
    }


    public int getOrder_id() {
        return order_id;
    }

    public void setOrder_id(int order_id) {
        this.order_id = order_id;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return order_id + "\t" + price;
    }
}
