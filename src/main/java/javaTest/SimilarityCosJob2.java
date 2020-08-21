package javaTest;

import java.util.ArrayList;
import java.util.List;

import Dec.function.WorkUDF;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * @Description spark计算余弦相似度
 * @Author yumingzhu
 * @Date 2019/1/14 9:43
 */
public class SimilarityCosJob2 {

	private static StructType inputStructType = DataTypes
			.createStructType(new StructField[] { DataTypes.createStructField("deviceId", DataTypes.StringType, true),
					DataTypes.createStructField("one", DataTypes.DoubleType, true),
					DataTypes.createStructField("tow", DataTypes.DoubleType, true),
					DataTypes.createStructField("three", DataTypes.DoubleType, true),
					DataTypes.createStructField("four", DataTypes.DoubleType, true),
					DataTypes.createStructField("five", DataTypes.DoubleType, true),
					DataTypes.createStructField("six", DataTypes.DoubleType, true),
					DataTypes.createStructField("seven", DataTypes.DoubleType, true) });

	private static StructType resultStructType = DataTypes
			.createStructType(new StructField[] { DataTypes.createStructField("deviceId", DataTypes.StringType, true),
					DataTypes.createStructField("cosSimilarity", DataTypes.DoubleType, true) });

	public static void main(String[] args) {
		SparkSession sparkSession = SparkSession.builder().master("local").appName("similarityCosJob").getOrCreate();
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());

		sparkSession.udf().register("cosUDF",new CosUDF(),DataTypes.DoubleType);

		JavaRDD<String> valueRDD = javaSparkContext.textFile("hdfs://192.168.1.7:8020/jiawei/similarityCos/value");
		JavaRDD<String> inputRDD = javaSparkContext.textFile("hdfs://192.168.1.7:8020/jiawei/similarityCos/input");

		JavaRDD<Row> valueRowRDD = valueRDD.map(createRowFunction);
		JavaRDD<Row> inputRowRDD = inputRDD.map(createRowFunction);

		Dataset<Row> valueDataset = sparkSession.createDataFrame(valueRowRDD, inputStructType);
		valueDataset.cache();
		valueDataset.createOrReplaceTempView("value");
		valueDataset.show(false);
		Dataset<Row> inputDataset = sparkSession.createDataFrame(inputRowRDD, inputStructType);
		inputDataset.cache();
		inputDataset.createOrReplaceTempView("input");
		inputDataset.show(false);

		Dataset<Row> leftJoinDataset = sparkSession.sql(
				"select in.deviceId,avg(in.cosSimilarity) avgCosSimilarity from" +
							" (select input.deviceId,cosUDF(input.one,input.tow,input.three,input.four,input.five,input.six,input.seven,value.one,value.tow,value.three,value.four,value.five,value.six,value.seven)  as cosSimilarity from " +
						    " input left join value on input.deviceId != value.deviceId or input.deviceId = value.deviceId) in " +
						" group by in.deviceId order by avgCosSimilarity desc limit 4 ");
		leftJoinDataset.show();
			// pow(input.seven,2)
		String cosString=" (input.one*value.one+input.tow*value.tow+input.three*value.three+input.four*value.four+input.five*value.five+input.six*value.six+input.seven*value.seven)/" +
						"(sqrt(pow(input.one,2)+pow(input.tow,2)+pow(input.three,2)+pow(input.four,2)+pow(input.five,2)+pow(input.six,2)+pow(input.seven,2))*sqrt(pow(value.one,2)+pow(value.tow,2)+pow(value.three,2)+pow(value.four,2)+pow(value.five,2)+pow(value.six,2)+pow(value.seven,2)))";
		Dataset<Row> leftJoinDataset2 = sparkSession.sql(
				"select in.deviceId,avg(in.cosSimilarity) avgCosSimilarity from" +
						" (select input.deviceId, "+cosString+"  as cosSimilarity from " +
						" input left join value on input.deviceId != value.deviceId or input.deviceId = value.deviceId) in "+
						" group by in.deviceId order by avgCosSimilarity desc limit 4 ");
		leftJoinDataset2.show();



	}

	/**
	 * 创建Row的function
	 */
	private static Function<String, Row> createRowFunction = new Function<String, Row>() {
		@Override
		public Row call(String v1) throws Exception {
			String[] values = v1.split(",");
			return RowFactory.create(values[0], Double.parseDouble(values[1]), Double.parseDouble(values[2]),
					Double.parseDouble(values[3]), Double.parseDouble(values[4]), Double.parseDouble(values[5]),
					Double.parseDouble(values[6]), Double.parseDouble(values[7]));
		}
	};
}
