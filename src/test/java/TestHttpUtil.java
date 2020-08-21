import Jan.HttpUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/2/21 17:16
 */
public class TestHttpUtil {
    private  static final Logger  logger=LoggerFactory.getLogger(TestHttpUtil.class);
    public static void main(String[] args) throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("name", "yumingzhu");
        params.put("fileName", "/xx/xx");
        params.put("population", "111");
        String res = HttpUtils.sendGet("http://localhost:8080//spark/dsp/getHttpsInfo", params);
        logger.info("请求返回响应 {}", res);
        JSONObject jsonObject = JSON.parseObject(res);
        String resCode = jsonObject.getString("ret");
        if (!"0".equals(resCode)) {
            throw new Exception(resCode);
        }

    }
}
