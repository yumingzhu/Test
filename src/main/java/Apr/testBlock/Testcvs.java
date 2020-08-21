package Apr.testBlock;

import com.csvreader.CsvReader;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/4/25 11:34
 */
public class Testcvs {
    public static void main(String[] args) {
        readCsv("G:\\test\\tag.csv");



    }
    // 读取csv文件的内容
    public static ArrayList<String> readCsv(String filepath) {
        try {

            ArrayList<String[]> csvList = new ArrayList<String[]>(); //用来保存数据  
            String csvFilePath = filepath;
            CsvReader reader = new CsvReader(csvFilePath,',',Charset.forName("gbk"));    //一般用这编码读就可以了

            reader.readHeaders(); // 跳过表头   如果需要表头的话，不要写这句。  

            while(reader.readRecord()){ //逐行读入除表头的数据      
                csvList.add(reader.getValues());
            }
            reader.close();

            for(int row=0;row<csvList.size();row++){

                String  cell = csvList.get(row)[0]; //取得第row行第0列的数据  
                System.out.println(cell);

            }


        }catch(Exception ex){
            System.out.println(ex);
        }
        return  null;
    }
}
