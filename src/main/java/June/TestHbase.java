package June;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/5/10 9:24
 */
public class TestHbase {
    private static Logger logger = LoggerFactory.getLogger(TestHbase.class);
    static Configuration conf;

    static Connection connection;

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "127.0.0.1:2181");
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        String tableName = "t1";
        Table table = connection.getTable(TableName.valueOf(tableName));// 获取表
        Result r1 = table.get(new Get(Bytes.toBytes("r1")));
        byte[] age = r1.getValue(Bytes.toBytes("cf"), Bytes.toBytes("age"));
        String ageStr = Bytes.toString(age);
        System.out.println("ageStr = " + ageStr);
    }

    private static void update2() throws IOException {
        String tableName = "two";
        Table table = connection.getTable(TableName.valueOf(tableName));// 获取表
        long start_time = System.currentTimeMillis();
        Get get = new Get(Bytes.toBytes("123456789"));
        Result result = table.get(get);

        Put put = new Put(Bytes.toBytes("123456789"));

        NavigableMap<byte[], byte[]> cf = result.getFamilyMap(Bytes.toBytes("cf"));
        for (Map.Entry<byte[], byte[]> entry : cf.entrySet()) {
           if(Bytes.toString(entry.getKey()).compareTo("A0020")>0){
               put.addColumn(Bytes.toBytes("cf"), entry.getKey(), Bytes.toBytes("2"));
           }else{
               put.addColumn(Bytes.toBytes("cf"), entry.getKey(), entry.getValue());
           }
        }
        Delete delete = new Delete(Bytes.toBytes("123456789"));
        table.delete(delete);
        table.put(put);
        System.out.println("耗时：" + (System.currentTimeMillis() - start_time));
    }

    private static void update1() throws IOException {
        String tableName = "two";
        Table table = connection.getTable(TableName.valueOf(tableName));// 获取表
        long start_time = System.currentTimeMillis();

        Get get = new Get(Bytes.toBytes("123456789"));
        Result result = table.get(get);
        Delete delete = new Delete(Bytes.toBytes("123456789"));
        String str = "A0000";
        int s = 20;
        for (int i = 20; i < 40; i++) {
            s++;
            String s1 = s + "";
            int length = s1.length();
            String substring = str.substring(0, str.length() - length);
            str = substring + s1;
            delete.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(str));
        }
        String str2 = "A0000";
        int s2 = 20;
        Put put = new Put(Bytes.toBytes("123456789"));
        for (int i = 20; i < 30; i++) {
            s2++;
            String s1 = s2 + "";
            int length = s1.length();
            String substring = str2.substring(0, str2.length() - length);
            str2 = substring + s1;
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(str2), Bytes.toBytes("2"));
        }
        table.delete(delete);
        table.put(put);
        System.out.println("耗时：" + (System.currentTimeMillis() - start_time));
    }

    private static void addtoHbase() throws IOException {
        String tableName = "two";
        Table table = connection.getTable(TableName.valueOf(tableName));// 获取表
        String str = "A0000";
        int s = 0000;
        Put put = new Put(Bytes.toBytes("123456789"));
        for (int i = 0; i < 40; i++) {
            s++;
            String s1 = s + "";
            int length = s1.length();
            String substring = str.substring(0, str.length() - length);
            str = substring + s1;
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(str), Bytes.toBytes("1"));

        }
        table.put(put);
    }


    private static String getResultString() throws IOException {
        String tableName = "aes_tag_data_value";
        Table table = connection.getTable(TableName.valueOf(tableName));// 获取表
        Get get = new Get(Bytes.toBytes("000493de89f3fb93fd1da603e7b48acf"));
        Result result = table.get(get);
        if (result.isEmpty()) {
            System.out.println("xxx");
        }
        String td_tag1 = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("td_tag1")));
        String td_tag2 = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("td_tag2")));
        String qm_tag1 = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("qm_tag1")));
        String qm_tag2 = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("qm_tag2")));
        String str = "";
        byte[] value = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("td_xtag1"));
        System.out.println("value = " + value);
        if (StringUtils.isNotBlank(td_tag1)) {
            str += td_tag1;
        }
        if (StringUtils.isNotBlank(td_tag2)) {
            str += td_tag1;
        }
        if (StringUtils.isNotBlank(qm_tag1)) {
            str += qm_tag1;
        }
        if (StringUtils.isNotBlank(qm_tag2)) {
            str += qm_tag2;
        }
        System.out.println(str);
        return str;
    }

    public static void getTag(Map<Integer, Tag> map) throws IOException {
        String tableName = "aes_tag_data_value";
        Table table = connection.getTable(TableName.valueOf(tableName));// 获取表
        Get get = new Get(Bytes.toBytes("00037057dc082d4210e50b3cfec01d0f"));
        Result result = table.get(get);
        CellScanner cellScanner = result.cellScanner();
        while (cellScanner.advance()) {
            Cell cell = cellScanner.current();
            String qualify = Bytes.toString(CellUtil.cloneQualifier(cell));
            Tag tag = map.get(qualify);
            System.out.println(tag);
        }

        table.close();
    }

    private static void testHbase() throws IOException {
        String tableName = "aes_tag_data_value";
        Table table = connection.getTable(TableName.valueOf(tableName));// 获取表
        Scan scan = new Scan();
        Get get = new Get(Bytes.toBytes("00004203de7c4abb96166df205574c9c"));
        ResultScanner scanner = table.getScanner(scan);
        Put put = new Put(Bytes.toBytes("123456789"));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("sw"), Bytes.toBytes("4"));
        table.put(put);
        table.close();
    }

}
