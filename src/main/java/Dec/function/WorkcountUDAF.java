package Dec.function;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

public class WorkcountUDAF   extends UserDefinedAggregateFunction {

   //指定输入字段及类型
    @Override
    public StructType inputSchema() {
        return DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("work",DataTypes.StringType,true)));
    }
    /***
     * 再进行聚合操作的时候所要处理的数据的结果类型
     * @return
     */
    @Override
    public StructType bufferSchema() {
        return DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("buffer_work", DataTypes.IntegerType, true)));

    }

   //最终返回的类型
    @Override
    public DataType dataType() {
        return DataTypes.IntegerType;

    }
    /**
     * 确保一致性 一般用true,用以标记针对给定的一组输入，UDAF是否总是生成相同的结果。
     */
    @Override
    public boolean deterministic() {
        return true;
    }
    //定义各个数据初始化规则
    @Override
    public void initialize(MutableAggregationBuffer buffer) {
       buffer.update(0,0);
    }
   // 定义每一组内聚合的规则
    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        buffer.update(0,buffer.getInt(0)+1);
    }
    //不同分区的聚合规则
    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
            buffer1.update(0,buffer1.getInt(0)+buffer2.getInt(0));
    }

    @Override
    public Object evaluate(Row buffer) {
        return buffer.getInt(0);
    }
}
