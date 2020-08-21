package Dec.DecTest;


import java.text.SimpleDateFormat;
import java.util.Date;

import com.mifmif.common.regex.Generex;
import org.json.JSONException;
import org.json.JSONObject;

public class CreateJson {
    public static void main(String[] args) {
        SimpleDateFormat  sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String date = sdf.format(new Date());
        System.out.println("date = [" + date+"] INFO ");
      //  String regex = "[1][34578][0-9]{9}";
       //   String regex = "([A-Fa-f0-9]{2}-){5}[A-Fa-f0-9]{2}";  //mac 正则表达式
      //     String regex="([0-9a-fA-F]{8})(([-][0-9a-fA-F]{4}){3})(([-][0-9a-fA-F]{12}){1})";  //idfa 正则表达式
         // String  regex="[0-9a-fA-F]{14,16}";    imei
          //String  regex="[0-9a-fA-F]{16}"; //androidid
          String  regex="([1-9]|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])(\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3}"; // ip

        Generex generex = new Generex(regex);
        // Generate random String
//        for (int i = 0; i <10 ; i++) {
//            String randomStr = generex.random();
//            System.out.println(randomStr);// a rand
//        }
        String json="{\"device\":{\"id\":\"7c2c1ace39ca9a9e2d20dc5721b32e4d\",\"carrier\":\"46001\",\"connectiontype\":1,\"devicetype\":2,\"os\":\"Andriod\",\"osv\":\"7.1.2\",\"ua\":\"GDTADNetClient-[Dalvik/2.1.0 (Linux; U; Android 7.1.2; Redmi 5 Plus MIUI/V9.6.3.0.NEGCNFD)]\",\"imei\":\"a7167aceaa00c64cf3d30612f3f8586d\",\"androidid\":\"28592da61f02648d7c5ab057f7a632f4\",\"mac\":\"02:00:00:00:00:00\",\"make\":\"HuaWei\",\"model\":\"Meat 8\",\"lan\":\"zh-CH\",\"h\":\"50\",\"w\":\"100\",\"geo\":{\"type\":1,\"lat\":31.01,\"lon\":36.1,\"country\":\"ISO\u00AD3166\u00AD1\u00ADalpha\u00AD3\",\"province\":\"广东省\",\"city\":\"广州\"}},\"id\":\"9d6411720a57cdbeda7e616bcc4d841e\",\"imp\":[{\"id\":\"3070906186403851\",\"bidfloor\":100,\"pid\":\"109\",\"ext\":{\"atype\":1},\"pmp\":{\"deals\":[{\"at\":1,\"id\":\"bf982f3f2\",\"price\":100}]},\"video\":{\"h\":360,\"maxduration\":120,\"minduration \":5,\"mimes \":[\"mp4 \",\"flv \"],\"w \":640},\"bidtype\":0,\"pos\":7}],\"app\":{\"appid\":\"10064\",\"bundle\":\"com.ireadercity.zhwll\",\"id\":\"100551\",\"name\":\"爱奇艺\",\"ver\":\"3.1.2\",\"keywords\":[\"资讯\",\"新闻\",\"安全\",\"澎湃新闻\",\"清理\",\"卫士\"]},\"test\":0,\"tmax\":100,\"user\":{\"id\":\"9d6411720a57cdbeda7e616bcc4\",\"yob\":\"1990\",\"gender\":\"M\",\"keywords\":[\"一点资讯\",\"你的全新公交应用\",\"开心消消乐2015\",\"贝贝特卖\",\"安卓优化大师\",\"360\",\"优化\"]},\"secure\":0}";
        System.out.println("json = " + json);
        try {
            JSONObject jsonObject = new JSONObject(json);
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }
}
