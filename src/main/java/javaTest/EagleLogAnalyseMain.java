package javaTest;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


/**
 * @Description  广本投放项目频次统计
 * @Author yumigzhu
 * @Date 2019/1/9 9:35
 */
public class EagleLogAnalyseMain {
	public static StructType EAGLE_LOG_SCHEMA = DataTypes
			.createStructType(new StructField[] { DataTypes.createStructField("projectId", DataTypes.StringType, true),
					DataTypes.createStructField("deviceId", DataTypes.StringType, true),
					DataTypes.createStructField("type", DataTypes.StringType, true),
					DataTypes.createStructField("time", DataTypes.StringType, true),
					DataTypes.createStructField("advsId", DataTypes.StringType, true)
			});
	//输出路径
	public static String HDFS_OUTPUT_PATH = "G:\\test";
	//读取路径
	public static String HDFS_INPUT_PATH = "G:\\test\\eagle.log";
	//	public static String HDFS_INPUT_PATH = "G:\\test\\eagle.log";

	public static void main(String[] args) {
		SparkSession sparkSession = SparkSession.builder().master("local").appName("EagleLogAnalyseMain").getOrCreate();
		JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
		//获取泛型
		final ProjectType ygpetC = ProjectType.YGPET_C;
		final ProjectType ygpetS = ProjectType.YGPET_S;
		final ProjectType yghevC = ProjectType.YGHEV_C;
		final ProjectType yghevS = ProjectType.YGHEV_S;
		final ProjectType adsC = ProjectType.ADS_C;
		final ProjectType adsS = ProjectType.ADS_S;
		//获取广本项目所对应的advsid
		final Broadcast<Map<Integer, Integer>> broadcastAdvsIdMap = javaSparkContext.broadcast(getEagleAdvsIdMap());

		JavaRDD<String> eagleLogRDD = javaSparkContext.textFile(HDFS_INPUT_PATH);
		//JavaRDD<String> eagleLogRDD = javaSparkContext.textFile("G:\\test\\eagle.log");
		Dataset<Row> eagleDF = geteagleDataset(sparkSession, broadcastAdvsIdMap, eagleLogRDD);
		eagleDF.registerTempTable("eagle_log");
		 Dataset<Row> sDF = sparkSession.sql("select projectId,deviceId ,advsId,time ,Row_Number() OVER(PARTITION BY projectId,deviceId,advsId ORDER BY time asc) rank  from eagle_log where type='s' ");
		 Dataset<Row> cDF = sparkSession.sql("select projectId,deviceId,advsId, time from eagle_log where type='c'");
		sDF.registerTempTable("s_log");
		sDF.show(false);
//		sparkSession.sql("select  * from s_log where rank=(select  rank from s_log where rank=1 and deviceId='0e75a2877bfe57c3c10ce087e39283d3' )").show();
		cDF.registerTempTable("c_log");
		sparkSession.sql(
				" select p.projectId ,p.rank ,count(p.rank) as count from  " +
						    " (select  s_log.projectId,s_log.deviceId,max(s_log.rank)as rank from   s_log join   c_log on (s_log.deviceId=c_log.deviceId) and (s_log.projectId=c_log.projectId)   where s_log.time<c_log.time group by s_log.projectId,s_log.deviceId)  p " +
						  " group by p.projectId , p.rank")
				.show();




//		//将结果注册成临时表
//		resultRDD.registerTempTable("result_eagle_log");
//		//根据ProjectType 将结果写入到hdfs
//		writeHDFS(sparkSession, ygpetC, ygpetS, yghevC, yghevS, adsC, adsS, HDFS_OUTPUT_PATH);
	}
	/***
	 *  对数据进行ElL , 将数据注册成临时表
	 * @param sparkSession
	 * @param broadcastAdvsIdMap
	 * @param eagleLogRDD
	 * @return
	 */
	private static Dataset<Row> geteagleDataset(SparkSession sparkSession,
			final Broadcast<Map<Integer, Integer>> broadcastAdvsIdMap, JavaRDD<String> eagleLogRDD) {
		JavaRDD<String> eaglemapRDD = eagleLogRDD.map(new Function<String, String>() {
			@Override
			public String call(String v1) {
				try {
					String[] line = v1.split(",");
					String type = line[0].split("\\:")[1];
					String time=line[1];
;					String deviceId = line[4];
					Integer advsId = Integer.valueOf(line[9]);
					Integer projectId = Integer.valueOf(line[11]);
					Map<Integer, Integer> advsIdMap = broadcastAdvsIdMap.getValue();

					if (advsIdMap.get(advsId) != null) {
						if (advsIdMap.get(advsId).equals(projectId)) {
							return projectId + "_" + deviceId+ "_" + type +"_"+time+"_"+advsId;
						}
					}
					return null;
				} catch (Exception e) {
					return null;
				}
			}
		});
		final SimpleDateFormat dateFormat = new SimpleDateFormat("yy-MM-dd hh:mm:sss");
		JavaRDD<String> eaglemFilterRDD = eaglemapRDD.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String v1) throws Exception {
				return v1 != null;
			}
		});
		JavaRDD<Row> rowEagleRDD = eaglemFilterRDD.map(new Function<String, Row>() {
			@Override
			public Row call(String v1) throws Exception {
				String[] split = v1.split("_");

				return RowFactory.create(split[0], split[1], split[2],split[3],split[4]);
			}
		});
		// 将结果注册成表
		return sparkSession.createDataFrame(rowEagleRDD, EAGLE_LOG_SCHEMA);
	}

	private static void writeHDFS(SparkSession sparkSession, ProjectType ygpetC, ProjectType ygpetS, ProjectType yghevC,
			ProjectType yghevS, ProjectType adsC, ProjectType adsS, String hdfsOutputPath) {
		// 查询当前 projectId 等于421 ， type  等于 c 的
		Dataset<Row> adsCDF = sparkSession.sql("select * from  result_eagle_log  where  projectId="
				+ adsC.getProjectId() + " and type='" + adsC.getType() + "' ");
		adsCDF.coalesce(1).write().format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
				.option("header", true).mode(SaveMode.Overwrite)
				.save(hdfsOutputPath + adsC.getProjectId() + "_" + adsC.getType());
		// 查询当前 projectId 等于421 ， type  等于 s 的
		Dataset<Row> adsSDF = sparkSession.sql("select * from  result_eagle_log  where  projectId="
				+ adsS.getProjectId() + " and type='" + adsS.getType() + "' ");
		adsSDF.coalesce(1).write().format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
				.option("header", true).mode(SaveMode.Overwrite)
				.save(hdfsOutputPath + adsS.getProjectId() + "_" + adsS.getType());

		// 查询当前 projectId 等于422 ， type  等于 c 的
		Dataset<Row> ygpetCDF = sparkSession.sql("select * from  result_eagle_log  where  projectId="
				+ ygpetC.getProjectId() + " and type='" + ygpetC.getType() + "' ");
		ygpetCDF.coalesce(1).write().format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
				.option("header", true).mode(SaveMode.Overwrite)
				.save(hdfsOutputPath + ygpetC.getProjectId() + "_" + ygpetC.getType());
		// 查询当前 projectId 等于422 ， type  等于 s 的
		Dataset<Row> ygpetsDF = sparkSession.sql("select * from  result_eagle_log  where  projectId="
				+ ygpetS.getProjectId() + " and type='" + ygpetS.getType() + "' ");
		ygpetsDF.coalesce(1).write().format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
				.option("header", true).mode(SaveMode.Overwrite)
				.save(hdfsOutputPath + ygpetS.getProjectId() + "_" + ygpetS.getType());

		// 查询当前 projectId 等于423 ， type  等于 c 的
		Dataset<Row> yghevCDF = sparkSession.sql("select * from  result_eagle_log  where  projectId="
				+ yghevC.getProjectId() + " and type='" + yghevC.getType() + "' ");
		yghevCDF.coalesce(1).write().format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
				.option("header", true).mode(SaveMode.Overwrite)
				.save(hdfsOutputPath + yghevC.getProjectId() + "_" + yghevC.getType());
		// 查询当前 projectId 等于423 ， type  等于 s 的
		Dataset<Row> yghevSDF = sparkSession.sql("select * from  result_eagle_log  where  projectId="
				+ yghevS.getProjectId() + " and type='" + yghevS.getType() + "' ");
		yghevSDF.coalesce(1).write().format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
				.option("header", true).mode(SaveMode.Overwrite)
				.save(hdfsOutputPath + yghevS.getProjectId() + "_" + yghevS.getType());
	}

	/***
	 *  获取符合项目id的广告位id
	 * @return
	 */
	public static Map<Integer, Integer> getEagleAdvsIdMap() {
		Map<Integer, Integer> map = new HashMap<>();
		// 奥德赛
		map.put(18326, 421);
		map.put(18324, 421);
		map.put(18322, 421);
		map.put(18198, 421);
		map.put(18196, 421);
		map.put(18194, 421);
		map.put(18192, 421);
		map.put(18190, 421);
		map.put(18188, 421);
		map.put(18187, 421);
		map.put(18185, 421);
		map.put(18183, 421);
		map.put(18181, 421);
		map.put(18179, 421);
		map.put(18177, 421);
		//雅阁HEV
		map.put(18332, 423);
		map.put(18330, 423);
		map.put(18328, 423);
		map.put(18175, 423);
		map.put(18173, 423);
		map.put(18171, 423);
		map.put(18169, 423);
		map.put(18167, 423);
		map.put(18165, 423);
		map.put(18163, 423);
		map.put(18161, 423);
		map.put(18159, 423);
		map.put(18157, 423);
		map.put(18155, 423);
		map.put(18153, 423);
		map.put(18151, 423);
		map.put(18149, 423);
		map.put(18147, 423);
		map.put(18145, 423);
		map.put(18143, 423);
		map.put(18141, 423);
		map.put(18139, 423);
		map.put(18137, 423);
		map.put(18135, 423);
		map.put(18133, 423);
		map.put(18131, 423);
		map.put(18129, 423);
		map.put(18127, 423);
		map.put(18125, 423);
		map.put(18123, 423);
		map.put(18121, 423);
		map.put(18119, 423);
		map.put(18117, 423);
		map.put(18115, 423);
		map.put(18113, 423);
		map.put(18111, 423);
		map.put(18109, 423);
		map.put(18107, 423);
		map.put(18105, 423);
		map.put(18103, 423);
		map.put(18101, 423);
		map.put(18099, 423);
		map.put(18097, 423);
		map.put(18095, 423);
		map.put(18093, 423);
		map.put(18091, 423);
		map.put(18089, 423);
		map.put(18087, 423);
		//雅阁PET
		map.put(18338, 422);
		map.put(18336, 422);
		map.put(18334, 422);
		map.put(18264, 422);
		map.put(18262, 422);
		map.put(18260, 422);
		map.put(18258, 422);
		map.put(18256, 422);
		map.put(18254, 422);
		map.put(18252, 422);
		map.put(18250, 422);
		map.put(18248, 422);
		map.put(18246, 422);
		map.put(18244, 422);
		map.put(18242, 422);
		map.put(18240, 422);
		map.put(18238, 422);
		map.put(18236, 422);
		map.put(18234, 422);
		map.put(18232, 422);
		map.put(18230, 422);
		map.put(18228, 422);
		map.put(18226, 422);
		map.put(18224, 422);
		map.put(18222, 422);
		map.put(18220, 422);
		map.put(18218, 422);
		map.put(18216, 422);
		map.put(18214, 422);
		map.put(18212, 422);
		map.put(18210, 422);
		map.put(18208, 422);
		map.put(18206, 422);
		map.put(18204, 422);
		map.put(18202, 422);
		map.put(18200, 422);
		return map;
	}

}
