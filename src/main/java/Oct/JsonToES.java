package Oct;

import org.apache.spark.network.client.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

public class JsonToES {
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonToES.class);

    public void jsonToES(File[] files) {

    }

    public static void main(String[] args) {
        JsonToES jsonToES = new JsonToES();
        File file = new File("D:\\JavaStudy\\elasticsearch-try\\src\\main\\resources\\json\\");
        // 目录下只有我需要读取的文件，故不再进行进一步处理
        File[] files = file.listFiles();
        jsonToES.jsonToES(files);
    }
}