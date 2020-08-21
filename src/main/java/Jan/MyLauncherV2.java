package Jan;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/2/18 14:48
 */
public class MyLauncherV2 {
    public static void main(String[] args) throws IOException, InterruptedException {

        HashMap env = new HashMap();
        //这两个属性必须设置
        env.put("HADOOP_CONF_DIR", "E:\\software\\hadoop-2.7.0\\etc\\hadoop");
        env.put("JAVA_HOME", "E:\\software\\Java\\jdk1.8.0_60");
        //可以不设置
        //env.put("YARN_CONF_DIR","");
        CountDownLatch countDownLatch = new CountDownLatch(1);
        //这里调用setJavaHome()方法后，JAVA_HOME is not set 错误依然存在

        SparkLauncher handle = new SparkLauncher(env)
                .setSparkHome("E:\\software\\spark-2.2.3-bin-hadoop2.7")
                .setAppResource("G:\\Test-1.0-SNAPSHOT.jar")
                .setMainClass("Dec.WorkCountSQL")
                .setMaster("local")
                .setVerbose(true);


        Process process =handle.launch();
        InputStreamReaderRunnable inputStreamReaderRunnable = new InputStreamReaderRunnable(process.getInputStream(), "input");
        Thread inputThread = new Thread(inputStreamReaderRunnable, "LogStreamReader input");
        inputThread.start();

        InputStreamReaderRunnable errorStreamReaderRunnable = new InputStreamReaderRunnable(process.getErrorStream(), "error");
        Thread errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader error");
        errorThread.start();

        System.out.println("Waiting for finish...");
        int exitCode = process.waitFor();
        System.out.println("Finished! Exit code:" + exitCode);

    }

}
