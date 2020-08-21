package Dec.function;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

public class ScoreAvgUDAF extends UserDefinedAggregateFunction  {
    @Override
    public StructType inputSchema() {
        return DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("chinese",DataTypes.IntegerType,true),DataTypes.createStructField("math",DataTypes.IntegerType,true)));

    }

    @Override
    public StructType bufferSchema() {
        return DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("sum", DataTypes.IntegerType, true) ,DataTypes.createStructField("count", DataTypes.IntegerType, true)));
    }

    @Override
    public DataType dataType() {
        return DataTypes.DoubleType;
    }

    @Override
    public boolean deterministic() {
        return true;
    }

    @Override
    public void initialize(MutableAggregationBuffer buffer) {
      buffer.update(0,0);
      buffer.update(1,0);

    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        buffer.update(0,buffer.getInt(0)+input.getInt(0)+input.getInt(1));
        buffer.update(1,buffer.getInt(1)+1);
    }

    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        buffer1.update(0,buffer1.getInt(0)+buffer2.getInt(0));
        buffer1.update(1,buffer1.getInt(1)+1);
    }

    @Override
    public Object evaluate(Row buffer) {
        return Double.valueOf (buffer.getInt(0) / buffer.getInt(1));
    }
}
