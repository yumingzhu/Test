package Utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Description  获取ip信息
 * @Author yumingzhu
 * @Date 2019//2
 */
public class IpUtils {

	private static Logger logger = LoggerFactory.getLogger(IpUtils.class);
	/**
	 * 获得连接mysqlJdbc连接配置文件
	 * @return
	 */
	public static Properties getMysqlJDBCProperties() {
		Properties properties = new Properties();
		properties.put("user", "root");
		properties.put("password", "gnova2017!@#");
		properties.put("driver", "com.mysql.jdbc.Driver");
		return properties;
	}
	private static String JDBC_URL="jdbc:mysql://192.168.1.6:3306/ymz-dsp?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&rewriteBatchedStatements=true&tinyInt1isBit=false";
	/***
	 *  获取ip范围的集合
	 * @param sparkSession
	 * @return   List(start_ip_end_ip_location)
	 */
	public static List<Map<String, Object>> getLocalInfo(SparkSession sparkSession) {
		Dataset<Row> tagCommentDF = sparkSession.read().jdbc(JDBC_URL, "ip_blocks",
				 getMysqlJDBCProperties());

		//将row拼装成我们想要的rdd
		JavaRDD<Map<String, Object>> ipRDD = tagCommentDF.javaRDD().map(new Function<Row, Map<String, Object>>() {
			@Override
			public Map<String, Object> call(Row row) throws Exception {
				Map<String, Object> ipMap = new HashMap<>();
				ipMap.put("start_ip", row.getLong(0));
				ipMap.put("end_ip", row.getLong(1));
				ipMap.put("location", row.getString(2));
				return ipMap;
			}
		});

		return ipRDD.collect();
	}

	/***
	 * 根据ip 的范围确定 location
	 * @param list
	 * @param ipStr
	 * @return
	 */
	public static String getLocalByIp(List<Map<String, Object>> list, String ipStr) {
		String location = "unknow";
		//解析ip
		Long ip = getRegionCode(ipStr);
		int left = 0;
		int right = list.size() - 1;
		int mid;
		Long start_ip;
		Long end_ip;
		while (left <= right) {
			mid = (left + right) / 2;
			Map<String, Object> map = list.get(mid);
			start_ip = (Long) map.get("start_ip");
			end_ip = (Long) map.get("end_ip");
			location = (String) map.get("location");
			if (start_ip <= ip && end_ip >= ip) {
				return location;
			} else if (start_ip > ip) {
				right = mid - 1;
			} else {
				left = mid + 1;
			}
		}
		return location;
	}

	/***
	 * 1 将IP 字段解析成 long 类型,
	 *
	 * @param ipStr
	 * @return
	 */
	private static Long getRegionCode(String ipStr) {
		Long ip_value = 0L;
		if (StringUtils.isBlank(ipStr) || !ipStr.contains(".")) {
			return ip_value;
		}
		try {
			long[] ip = new long[4];
			// 先找到IP地址字符串中.的位置
			int position1 = ipStr.indexOf(".");
			int position2 = ipStr.indexOf(".", position1 + 1);
			int position3 = ipStr.indexOf(".", position2 + 1);
			if (position1 == -1 || position2 == -1 || position3 == -1) {
				return ip_value;
			}
			// 将每个.之间的字符串转换成整型
			ip[0] = Long.parseLong(ipStr.substring(0, position1));
			ip[1] = Long.parseLong(ipStr.substring(position1 + 1, position2));
			ip[2] = Long.parseLong(ipStr.substring(position2 + 1, position3));
			ip[3] = Long.parseLong(ipStr.substring(position3 + 1));
			ip_value = (ip[0] << 24) + (ip[1] << 16) + (ip[2] << 8) + ip[3];
			return ip_value;
		} catch (Exception e) {
			logger.error("IpUtils  getRegionCode ip:{} ", ipStr, e.getMessage(), e);
		}
		return null;
	}

	/***
	 *  获取 所有的location 所对应的城市信息   国家，省份，城市  ， 城市的排名
	 * @param sparkSession
	 * @return   Map(start_ip_end_ip,location)
	 */
	public static Map<String, Map<String, String>> getCityInfos(SparkSession sparkSession) {
		Dataset<Row> tagCommentDF = sparkSession.read().jdbc(JDBC_URL, "area",
			getMysqlJDBCProperties());

		Map<String, Map<String, String>> resultMap = new HashMap<>();
		for (Row row : tagCommentDF.collectAsList()) {
			Map<String, String> valueMap = new HashMap<>();
			valueMap.put("country_name", row.getString(5));
			valueMap.put("province_name", row.getString(6));
			valueMap.put("city_name", row.getString(7));
			valueMap.put("city_level", row.getString(15));
			resultMap.put(row.getString(4), valueMap);
		}

		return resultMap;
	}

	public static void main(String[] args) {
		SparkSession sparkSession = SparkSession.builder().master("local").getOrCreate();

		Dataset<Row> tagCommentDF2 = sparkSession.read().jdbc(JDBC_URL, "ip_blocks",getMysqlJDBCProperties());
		System.out.println("tagCommentDF2.count() = " + tagCommentDF2.count());
		Map<String, String> map=new HashMap<>();
		map.put("user", "root");
		map.put("password", "gnova2017!@#");
		map.put("driver", "com.mysql.jdbc.Driver");
		map.put("url",JDBC_URL);
		map.put("dbtable","(select start_ip, end_ip,location  from ip_blocks) temp");
		Dataset<Row> ip_blocks = sparkSession.read().options(map).format("jdbc").load();
		System.out.println("ip_blocks = " + ip_blocks.count());

		//将row拼装成我们想要的rd
//		ipRDD.repartition(1).saveAsTextFile("G:\\bid_logs\\ip_blocks");

	}

}
