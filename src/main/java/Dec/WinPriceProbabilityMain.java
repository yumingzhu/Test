package Dec;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;



/**
 * @author linhua
 * @Description: 赢价概率的计算
 * 输入：订单id  竞价模式   最低价  最高价
 * 统计数据源：有明确输赢记录的流量
 *      数据格式：订单id ,广告位id ,出价，成交价
 *      （有成交价是赢，没有成交价是输）
 * 输出：订单id,广告位id,出价,概率
 *       10123 ,23045  ,100 ,0.01
 * @date 2019/1/18 10:53
 */
public class WinPriceProbabilityMain {

	private static StructType structType = DataTypes
			.createStructType(new StructField[] { DataTypes.createStructField("orderId", DataTypes.StringType, true),
					DataTypes.createStructField("advId", DataTypes.StringType, true),
					DataTypes.createStructField("price", DataTypes.DoubleType, true),
					DataTypes.createStructField("dealPrice", DataTypes.DoubleType, true),
					DataTypes.createStructField("win", DataTypes.IntegerType, true),
					DataTypes.createStructField("incrPrice", DataTypes.DoubleType, true) });

	public static void main(String[] args) {
		//订单id
		String orderId ="15";
		//竞价模式 两种 出价=成交价，出价>成交价
		String bidMode = "2";
		//最低价
		double lowPrice = Double.parseDouble("2000");
		//最高价
		double topPrice = Double.parseDouble("4000");
		//输出文件名
//		String outputFileName = args[4];
//		//输入文件名
//		String inputFileName = args[5];
		//出价递增单元
		double priceUnit = 1;
		//梯度价格列表
		List<Double> orderPriceList = new ArrayList<>();
		while (lowPrice < topPrice) {
			orderPriceList.add(lowPrice);
			lowPrice += priceUnit;
		}
		orderPriceList.add(topPrice);

		SparkSession spark = SparkSession.builder().master("local").appName("winPriceProbability").getOrCreate();
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		final Broadcast<List<Double>> orderPriceListBroadcast = jsc.broadcast(orderPriceList);

		String inputPath = "G:\\bidLogs\\part-00000-42a8cf97-7a2d-4b30-bf7f-4c4aa3690f24.csv";

		JavaRDD<String> inputDataRDD = jsc.textFile(inputPath);
		JavaRDD<Row> rowJavaRDD = inputDataRDD.flatMap(new FlatMapFunction<String, Row>() {

			@Override
			public Iterator<Row> call(String s) throws Exception {
				List<Double> doubleList = orderPriceListBroadcast.value();
				List<Row> resultList = new ArrayList<>();
				String[] split = s.split(",");
				// split [订单id ,广告位id ,出价，成交价]
				try {
					for (Double incrPri : doubleList) {
						//成交价
						Double dealPrice = null;
						//竞价输赢，1赢 0输
						Integer win = 0;
						if (split.length > 3 && StringUtils.isNotBlank(split[3]) && !split[3].equals("winPrice")) {
							dealPrice = Double.parseDouble(split[3]);
							win = 1;
							// [订单id ,广告位id ,出价，成交价,输/赢，梯度价格]
							Row row = RowFactory.create(split[0], split[1], Double.parseDouble(split[2]), dealPrice,
									win, incrPri);
							resultList.add(row);
						}

						if (split.length == 3) {
							Row row = RowFactory.create(split[0], split[1], Double.parseDouble(split[2]), null, win,
									incrPri);
							resultList.add(row);
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}

				return resultList.iterator();
			}
		});

		Dataset<Row> bidDataset = spark.createDataFrame(rowJavaRDD, structType);

		bidDataset.createOrReplaceTempView("bidData_table");

		bidDataset.cache();

		//出价小于或等于2的总量  分母1
		Dataset<Row> fm1 = spark.sql(
				"select orderId,advId,incrPrice,sum(case WHEN price<=incrPrice then 1 else 0 end) as fm1_c from bidData_table "
						+ "where orderId='" + orderId + "' group by orderId,advId,incrPrice");
		fm1.createOrReplaceTempView("fm1_table");

		//出价大于2输的量  分母2
		Dataset<Row> fm2 = spark.sql(
				"select orderId,advId,incrPrice,sum(case WHEN price>incrPrice then 1 else 0 end) as fm2_c from bidData_table "
						+ "where orderId='" + orderId + "' and win=0 group by orderId,advId,incrPrice");
		fm2.createOrReplaceTempView("fm2_table");

		Dataset<Row> fz;
		if (bidMode.equals("1")) {
			//第一竞价：出价=成交价
			//出价小于或等于2赢的量  分子
			fz = spark.sql(
					"select orderId,advId,incrPrice,sum(case WHEN price<=incrPrice then 1 else 0 end) as fz_c from bidData_table "
							+ "where orderId='" + orderId + "' and win=1 group by orderId,advId,incrPrice");
		} else {
			//第二竞价：出价>成交价
			//（成交价小于2赢的量）  分子
			fz = spark.sql(
					"select orderId,advId,incrPrice,sum(case WHEN dealPrice<incrPrice then 1 else 0 end) as fz_c from bidData_table "
							+ "where orderId='" + orderId + "' and win=1 group by orderId,advId,incrPrice");

		}
		fz.createOrReplaceTempView("fz_table");

		//计算赢价概率
		Dataset<Row> resultData = spark.sql(
				"select fz.orderId,fz.advId,fz.incrPrice,IFNULL(fz.fz_c/(IFNULL(fm1.fm1_c,0)+IFNULL(fm2.fm2_c,0)),0) as pr  ,fz.fz_c,fm1.fm1_c,fm2.fm2_c  from fz_table fz "
						+ "left join fm1_table fm1 on (fz.orderId=fm1.orderId and fz.advId=fm1.advId and fz.incrPrice=fm1.incrPrice) "
						+ "left join fm2_table fm2 on (fz.orderId=fm2.orderId and fz.advId=fm2.advId and fz.incrPrice=fm2.incrPrice)");
		resultData.coalesce(1).write().csv("G:\\bidLogs\\11");

		jsc.stop();
		spark.stop();
	}
}
