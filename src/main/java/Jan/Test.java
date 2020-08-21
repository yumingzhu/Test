package Jan;

import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * @Description TODO
 * @Author yumingzhu
 * @Date 2019/1/2 14:34
 */
public class Test {
	private static final Logger logger = LoggerFactory.getLogger(Test.class);

	public static void main(String[] args) throws Exception {
		String startTime = "2019-03-10";
		String endTime = "2019-03-11";
		String inputPath ="hdfs://192.168.1.7:8020/data/log_data/eagle_log/";

		String json = "[{\"startTime\":\"2018-06-01\",\"endTime\":\"2019-01-02\",\"projectId\":448,\"advId\":[18448,18449],\"type\":1},{\"startTime\":\"2019-01-01\",\"endTime\":\"2019-01-03\",\"projectId\":449,\"advId\":[18447,18449],\"type\":2}]";




//				JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
//
//				JavaRDD<String> stringJavaRDD = jsc.textFile(PropertiesUtils.HDFS_DEFAULT_FS + "/jiawei/ymz_data/2019-01-02/*");
//
//				long count = stringJavaRDD.count();
//				System.out.println(count);
//		System.out.println("inputPath = " + inputPath);

		String inputPathS = getInputPathS(startTime, endTime, inputPath);
		System.out.println(inputPathS);
	}

	public static String getInputPathS(String startTime, String endTime, String inputPath) throws Exception {
		//2-1   --  3-1
		Calendar calenda1 = Calendar.getInstance();
		StringBuilder str = new StringBuilder();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		Date date1 = format.parse(startTime);
		Date date2 = format.parse(endTime);
		calenda1.setTime(date1);

		Date currentDate = new Date(); //获取当前时间   用于判断最后时间 是否大于 当前时间， 如果是则跳出循环
		Configuration configuration = new Configuration();
		FileSystem fileSystem = FileSystem.get(configuration);

		while (date1.getTime()<=date2.getTime()) {
			calenda1.add(Calendar.DATE, 1);
			String  logDir="eagle."+format.format(calenda1.getTime())+"*";
			FileStatus[] fileStatus = fileSystem
				.globStatus(new Path(inputPath+logDir));
			for (FileStatus status : fileStatus) {
				str.append(status.getPath()).append(",");
			}
			if (calenda1.getTimeInMillis() >= currentDate.getTime()) {
				break;
			}
		}
		String path = "";
		if (StringUtils.isBlank(str)) {
			return path;
		}
		path = str.substring(0, str.length() - 1);
		return str.toString();

	}

	/***
	 *
	 * @param
	 * @return
	 */
	private static Map<String, List<String>> getProAdvMap(String json) {
		Map<String, List<String>> map = new HashMap<>();
		JSONArray info = null;
		try {
			info = JSONObject.parseArray(json);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("json 解析出问题了 ！！！ {}", json);
			System.exit(0);
		}

		for (int i = 0; i < info.size(); i++) {
			List<String> timelist = new ArrayList<>();
			JSONObject jsonObject = info.getJSONObject(i);
			String projectId = jsonObject.getString("projectId");
			JSONArray advIds = jsonObject.getJSONArray("advId");

			String startTime = jsonObject.getString("startTime");
			String endTime = jsonObject.getString("endTime");
			int type = jsonObject.getInteger("type");
			timelist.add(startTime);
			timelist.add(endTime);
			for (int j = 0; j < advIds.size(); j++) {
				Integer advId = advIds.getInteger(j);
				if (type == 1) {
					map.put(projectId + "_" + advId + "_s", timelist);
				} else {
					map.put(projectId + "_" + advId + "_c", timelist);
				}
			}

		}
		return map;
	}
}
