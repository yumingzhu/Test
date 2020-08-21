package javaTest;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * @Description
 * @Author zhangjw
 * @Date 2019/1/9 14:10
 */
public class DeviceIdTimeJob {

	private static StructType structType = DataTypes
			.createStructType(new StructField[] { DataTypes.createStructField("projectId", DataTypes.StringType, true),
					DataTypes.createStructField("advId", DataTypes.StringType, true),
					DataTypes.createStructField("type", DataTypes.StringType, true),
					DataTypes.createStructField("deviceId", DataTypes.StringType, true),
					DataTypes.createStructField("time", DataTypes.StringType, true) });

	private static String HDFS_OUTPUT_PATH = "hdfs://192.168.1.7:8020/jiawei/result2/";

	public static void main(String[] args) {
		SparkSession sparkSession = SparkSession.builder().master("local").appName("deviceIdTimeJob").getOrCreate();
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());

		Map<String, String> adsIdAndProjectIdMap = getEagleAdvsIdMap();
		Broadcast<Map<String, String>> idMap = javaSparkContext.broadcast(adsIdAndProjectIdMap);

		JavaRDD<String> inputRDD = javaSparkContext.textFile("G:\\test\\eagle.log");
		JavaRDD<Row> mapRDD = createMapRDD(inputRDD, idMap);
		JavaRDD<Row> filterRDD = mapRDD.filter(new Function<Row, Boolean>() {
			@Override
			public Boolean call(Row v1) throws Exception {
				return v1 != null;
			}
		});

		Dataset<Row> dataSet = sparkSession.createDataFrame(filterRDD, structType);
		dataSet.cache();
		dataSet.createOrReplaceTempView("final_table");

		Dataset<Row> clickDataSet = sparkSession.sql(
				"select projectId,advId,deviceId,min(time) time from final_table where type='c' group by projectId,"
						+ "advId,deviceId");
		clickDataSet.registerTempTable("click_table");

		Dataset<Row> showDataSet = sparkSession
				.sql("select projectId,advId,deviceId,time from final_table where type='s'");
		showDataSet.createOrReplaceTempView("show_table");

		Dataset<Row> countDataSet = sparkSession
				.sql("select c.deviceId deviceId,c.advId advId,c.projectId projectId,count(1) show from show_table s "
						+ "right join click_table c on (s.projectId = c.projectId) AND (s.deviceId = c.deviceId) and (s.advId=c.advId) "
						+ "and (s.time<=c.time) GROUP BY c.projectId,c.deviceId,c.time,c.advId");
		countDataSet.createOrReplaceTempView("count_table");

		Dataset<Row> resultDataSet = sparkSession
				.sql("select projectId,advId,show,count(1) num from count_table group by projectId,show,advId");
		resultDataSet.createOrReplaceTempView("result_table");
        resultDataSet.show();
//		createCsvAndSave(sparkSession);
		sparkSession.close();
		javaSparkContext.close();
	}

	/**
	 * 创建包含数据的Row的方法
	 * @param inputRDD
	 * @param idMapBroad
	 * @return
	 */
	private static JavaRDD<Row> createMapRDD(JavaRDD<String> inputRDD,
			final Broadcast<Map<String, String>> idMapBroad) {
		JavaRDD<Row> mapRDD = inputRDD.map(new Function<String, Row>() {
			@Override
			public Row call(String value) {
				try {
					String[] values = value.split(",");
					String type = values[0].split(":")[1];
					String deviceId = values[4];
					String time = values[1];
					String advId = values[9];
					String projectId = values[11];
					if (!(StringUtils.isBlank(type) || StringUtils.isBlank(deviceId) || StringUtils.isBlank(advId)
							|| StringUtils.isBlank(projectId))) {
						Map<String, String> idMap = idMapBroad.getValue();
						if (StringUtils.isNotBlank(idMap.get(advId)) && idMap.get(advId).equals(projectId)) {
							return RowFactory.create(projectId, advId, type, deviceId, time);
						}
					}
				} catch (Exception e) {
					return null;
				}

				return null;
			}
		});

		return mapRDD;
	}

	/**
	 * 创建并保存csv文件
	 * @param sparkSession
	 */
	private static void createCsvAndSave(SparkSession sparkSession) {
		sparkSession.sql("select projectId,advId,show,num from result_table where projectId='421'").coalesce(1).write()
				.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").option("header", true)
				.mode(SaveMode.Overwrite).save(HDFS_OUTPUT_PATH + "/421");
		sparkSession.sql("select projectId,advId,show,num from result_table where projectId='422'").coalesce(1).write()
				.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").option("header", true)
				.mode(SaveMode.Overwrite).save(HDFS_OUTPUT_PATH + "/422");
		sparkSession.sql("select projectId,advId,show,num from result_table where projectId='423'").coalesce(1).write()
				.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").option("header", true)
				.mode(SaveMode.Overwrite).save(HDFS_OUTPUT_PATH + "/423");
	}

	/**
	 * 创建adsId对应的项目id的关系列表
	 * @return
	 */
	private static Map<String, String> getEagleAdvsIdMap() {
		Map<String, String> map = new HashMap<>();
		// 奥德赛
		map.put("18326", "421");
		map.put("18324", "421");
		map.put("18322", "421");
		map.put("18198", "421");
		map.put("18196", "421");
		map.put("18194", "421");
		map.put("18192", "421");
		map.put("18190", "421");
		map.put("18188", "421");
		map.put("18187", "421");
		map.put("18185", "421");
		map.put("18183", "421");
		map.put("18181", "421");
		map.put("18179", "421");
		map.put("18177", "421");
		//雅阁HEV
		map.put("18332", "423");
		map.put("18330", "423");
		map.put("18328", "423");
		map.put("18175", "423");
		map.put("18173", "423");
		map.put("18171", "423");
		map.put("18169", "423");
		map.put("18167", "423");
		map.put("18165", "423");
		map.put("18163", "423");
		map.put("18161", "423");
		map.put("18159", "423");
		map.put("18157", "423");
		map.put("18155", "423");
		map.put("18153", "423");
		map.put("18151", "423");
		map.put("18149", "423");
		map.put("18147", "423");
		map.put("18145", "423");
		map.put("18143", "423");
		map.put("18141", "423");
		map.put("18139", "423");
		map.put("18137", "423");
		map.put("18135", "423");
		map.put("18133", "423");
		map.put("18131", "423");
		map.put("18129", "423");
		map.put("18127", "423");
		map.put("18125", "423");
		map.put("18123", "423");
		map.put("18121", "423");
		map.put("18119", "423");
		map.put("18117", "423");
		map.put("18115", "423");
		map.put("18113", "423");
		map.put("18111", "423");
		map.put("18109", "423");
		map.put("18107", "423");
		map.put("18105", "423");
		map.put("18103", "423");
		map.put("18101", "423");
		map.put("18099", "423");
		map.put("18097", "423");
		map.put("18095", "423");
		map.put("18093", "423");
		map.put("18091", "423");
		map.put("18089", "423");
		map.put("18087", "423");
		//雅阁PET
		map.put("18338", "422");
		map.put("18336", "422");
		map.put("18334", "422");
		map.put("18264", "422");
		map.put("18262", "422");
		map.put("18260", "422");
		map.put("18258", "422");
		map.put("18256", "422");
		map.put("18254", "422");
		map.put("18252", "422");
		map.put("18250", "422");
		map.put("18248", "422");
		map.put("18246", "422");
		map.put("18244", "422");
		map.put("18242", "422");
		map.put("18240", "422");
		map.put("18238", "422");
		map.put("18236", "422");
		map.put("18234", "422");
		map.put("18232", "422");
		map.put("18230", "422");
		map.put("18228", "422");
		map.put("18226", "422");
		map.put("18224", "422");
		map.put("18222", "422");
		map.put("18220", "422");
		map.put("18218", "422");
		map.put("18216", "422");
		map.put("18214", "422");
		map.put("18212", "422");
		map.put("18210", "422");
		map.put("18208", "422");
		map.put("18206", "422");
		map.put("18204", "422");
		map.put("18202", "422");
		map.put("18200", "422");
		return map;
	}

}
