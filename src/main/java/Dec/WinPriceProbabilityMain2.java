package Dec;

import java.util.*;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Int;
import scala.Tuple2;

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
public class WinPriceProbabilityMain2 {

	private static StructType structType = DataTypes
			.createStructType(new StructField[] { DataTypes.createStructField("orderId", DataTypes.StringType, true),
					DataTypes.createStructField("advId", DataTypes.StringType, true),
					DataTypes.createStructField("price", DataTypes.DoubleType, true),
					DataTypes.createStructField("dealPrice", DataTypes.DoubleType, true),
					DataTypes.createStructField("win", DataTypes.IntegerType, true),
					DataTypes.createStructField("incrPrice", DataTypes.DoubleType, true) });

	public static void main(String[] args) {
		//订单id
		String orderId = "15";
		//竞价模式 两种 出价=成交价，出价>成交价
		String bidMode = "2";
		//最低价
		double lowPrice = Double.parseDouble("1000");
		//最高价
		double topPrice = Double.parseDouble("10000");
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
		JavaPairRDD<String, List<Integer>> baseRDD = inputDataRDD
				.mapToPair(new PairFunction<String, String, List<Integer>>() {
					@Override
					public Tuple2<String, List<Integer>> call(String s) throws Exception {
						List<Integer> list = new ArrayList<>();
						String[] split = s.split(",");
						if (orderId.equals(split[0])) {
							String key = split[0] + "_" + split[1];
							if (split.length > 3 && StringUtils.isNotBlank(split[3]) && !split[3].equals("winPrice")) {
								list.add(1);
								list.add(Integer.valueOf(split[2]));
								list.add(Integer.valueOf(split[3]));
							}
							if (split.length == 3) {
								list.add(0);
								list.add(Integer.valueOf(split[2]));
								list.add(0);
							}
							return new Tuple2<String, List<Integer>>(key, list);
						}
						return null;
					}
				}).filter(new Function<Tuple2<String, List<Integer>>, Boolean>() {
					@Override
					public Boolean call(Tuple2<String, List<Integer>> v1) throws Exception {
						return v1 != null;
					}
				});
        JavaPairRDD<String, ArrayList<ArrayList<Integer>>> order_adv_RDD = baseRDD.aggregateByKey(new ArrayList<ArrayList<Integer>>(), new Function2<ArrayList<ArrayList<Integer>>, List<Integer>, ArrayList<ArrayList<Integer>>>() {
            @Override
            public ArrayList<ArrayList<Integer>> call(ArrayList<ArrayList<Integer>> v1, List<Integer> v2) throws Exception {
                v1.add((ArrayList<Integer>) v2);
                return v1;
            }
        }, new Function2<ArrayList<ArrayList<Integer>>, ArrayList<ArrayList<Integer>>, ArrayList<ArrayList<Integer>>>() {
            @Override
            public ArrayList<ArrayList<Integer>> call(ArrayList<ArrayList<Integer>> v1, ArrayList<ArrayList<Integer>> v2) throws Exception {
                v1.addAll(v2);
                return v1;
            }
        });
        JavaRDD<Tuple2<String, Double>> tuple2JavaRDD = order_adv_RDD
                .flatMap(new FlatMapFunction<Tuple2<String, ArrayList<ArrayList<Integer>>>, Tuple2<String, Double>>() {
                    @Override
                    public Iterator<Tuple2<String, Double>> call(Tuple2<String, ArrayList<ArrayList<Integer>>> v1)
                            throws Exception {
                        List<Double> value = orderPriceListBroadcast.value();
                        List<Tuple2<String, Double>> list = new ArrayList<>();
                        for (Double doule : value) {//标准预估价格
                            int winCount = 0; //分子  赢的量  ，  出价小于或等于2赢的总量|成交价小于2赢的量
                            int fairCount = 0; //出价大于 大于标准输的量;
                            int sumCount = 0;//出价小于或等于标准的总量
                            for (ArrayList<Integer> temp : v1._2) {
                                //list 中  0 为是否赢价(1赢，0输)，  1 出价价格，  2为赢价价格
                                if (temp.get(1) <= doule) {
                                    sumCount++;
                                }
                                if (temp.get(1) > doule && temp.get(0) == 0) {
                                    fairCount++;
                                }
                                //判断竞价类型  获取赢的量
                                if (bidMode.equals("1") && temp.get(1) <= doule && temp.get(0) == 1) {
                                    winCount++;
                                } else if (bidMode.equals("2") && temp.get(2) < doule && temp.get(0) == 1) {
                                    winCount++;
                                }
                            }
                            int sum = sumCount + fairCount;
                            if (sum == 0) {
                                list.add(new Tuple2<String, Double>(v1._1 + "_" + doule, 0.0));
                            } else {
                                double probability = Double.valueOf(winCount) / Double.valueOf(sum);
                                list.add(new Tuple2<String, Double>(v1._1 + "_" + doule, probability));
                            }
                        }
                        return list.iterator();
                    }
                });
        tuple2JavaRDD.foreach(new VoidFunction<Tuple2<String, Double>>() {
            @Override
            public void call(Tuple2<String, Double> value) throws Exception {
                System.out.println(value._1+"\t"+value._2);
            }
        });
        jsc.stop();
		spark.stop();
	}
}
