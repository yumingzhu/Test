package Dec.Accumulator;

import org.apache.spark.AccumulatorParam;

public class WorkAccumulator  implements AccumulatorParam<Integer> {


    @Override//每次调用count.add()计算规则
    public Integer addAccumulator(Integer t1, Integer t2) {
        return  (t1+t2);
    }

    @Override//各个分区时间进行累计的规则
    public Integer addInPlace(Integer r1, Integer r2) {
        return (r1+r2);
    }

    @Override
    public Integer zero(Integer initialValue) {
        return 0;
    }
}
