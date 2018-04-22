package partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 1363157993055	13560439658	C4-17-FE-BA-DE-D9:CMCC	120.196.100.99			18	15	1116	954	200
 * 1363157992093	13560439658	C4-17-FE-BA-DE-D9:CMCC	120.196.100.99			15	9	918	4938	200
 *
 * <13560439658, [ FlowBean(1116, 954, 0), FlowBean(918, 4938, 0)]
 */
public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    FlowBean resultBean = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sumUpFlow = 0;
        long sumDownFlow = 0;

        for (FlowBean flowBean : values) {
            sumUpFlow += flowBean.getUpFlow();
            sumDownFlow += flowBean.getDownFlow();
        }

        //将结果封装到对象
        resultBean.setUpFlow(sumUpFlow);
        resultBean.setDownFlow(sumDownFlow);
        resultBean.setSumFlow(sumUpFlow + sumDownFlow);

        //将结果带出
        context.write(key, resultBean);
    }
}
