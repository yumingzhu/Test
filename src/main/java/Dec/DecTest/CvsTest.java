package Dec.DecTest;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CvsTest {
    public static void main(String[] args) {
        //自定义数组
        ArrayList<ArrayList<String>> alldata=new ArrayList<ArrayList<String>>();
        alldata.add(new ArrayList<String>(Arrays.asList("1","11","111")));  //添加一行
        alldata.add(new ArrayList<String>(Arrays.asList("2","22","222")));  //添加一行
        alldata.add(new ArrayList<String>(Arrays.asList("3","33","333")));  //添加一行
        //保存成csv文件
        Array2CSV(alldata,"G:\\test\\test.csv");
      //  读取csv文件
        ArrayList<ArrayList<String>> alldata1=CSV2Array("G:\\test\\test.csv");
        //遍历数组
        for (ArrayList<String> arrayList : alldata1) {
            for (String string : arrayList) {
                System.out.println(string);
            }
        }

    }
    //导出到csv文件
    public static void Array2CSV(ArrayList<ArrayList<String>> data, String path)
    {
        try {
            BufferedWriter out =new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path),"UTF-8"));
            for (int i = 0; i < data.size(); i++)
            {
                ArrayList<String> onerow=data.get(i);
                for (int j = 0; j < onerow.size(); j++)
                {
                    out.write(DelQuota(onerow.get(j)));
                    out.write(",");
                }
                out.newLine();
            }
            out.flush();
            out.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    public static String DelQuota(String str)
    {
        String result = str;
        String[] strQuota = { "~", "!", "@", "#", "$", "%", "^", "&", "*", "(", ")", "`", ";", "'", ",", ".", "/", ":", "/,", "<", ">", "?" };
        for (int i = 0; i < strQuota.length; i++)
        {
            if (result.indexOf(strQuota[i]) > -1)
                result = result.replace(strQuota[i], "");
        }
        return result;
    }
    public static ArrayList<ArrayList<String>> CSV2Array(String path)
    {
        try {
            BufferedReader in =new BufferedReader(new InputStreamReader(new FileInputStream(path),"UTF-8"));
            ArrayList<ArrayList<String>> alldata=new ArrayList<ArrayList<String>>();
            String line;
            String[] onerow;
            while ((line=in.readLine())!=null) {
                onerow = line.split(",");  //默认分割符为逗号，可以不使用逗号
                List<String> onerowlist = Arrays.asList(onerow);
                ArrayList<String> onerowaArrayList = new ArrayList<String>(onerowlist);
                alldata.add(onerowaArrayList);
            }
            in.close();
            return alldata;
        } catch (Exception e) {
            return null;
        }

    }


}
