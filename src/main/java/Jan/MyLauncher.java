package Jan;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/2/18 11:26
 */
public class MyLauncher {
    public static void main(String[] args) throws Exception {
        SparkAppHandle handle = new SparkLauncher()
                .setAppResource("G:\\Test-1.0-SNAPSHOT.jar")
                .setMainClass("Dec.WorkCountSQL")
                .setMaster("local")
                .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
                .startApplication();

        // Use handle API to monitor / control application.
    }

}
