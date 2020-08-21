package Jan;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/2/18 11:26
 */
public class LauncherAppV {
    public static void main(String[] args) throws Exception {


//        HashMap env = new HashMap();
//        //这两个属性必须设置
//        env.put("HADOOP_CONF_DIR", "E:\\software\\hadoop-2.7.0\\etc\\hadoop");
//        env.put("JAVA_HOME", "E:\\software\\Java\\jdk1.8.0_60");
        //可以不设置
        //env.put("YARN_CONF_DIR","");
        CountDownLatch countDownLatch = new CountDownLatch(1);
        //这里调用setJavaHome()方法后，JAVA_HOME is not set 错误依然存在
        SparkAppHandle handle = new SparkLauncher()
                .setSparkHome("E:\\software\\spark-2.2.3-bin-hadoop2.7")
                .setAppResource("G:\\Test-1.0-SNAPSHOT.jar")
                .setMainClass("Dec.WorkCountSQL")
                .setMaster("local")
                .setConf("spark.app.id", "11222")

//                .setConf("spark.driver.memory", "512M")
                .setVerbose(true).startApplication(new SparkAppHandle.Listener() {
                    //这里监听任务状态，当任务结束时（不管是什么原因结束）,isFinal（）方法会返回true,否则返回false
                    @Override
                    public void stateChanged(SparkAppHandle sparkAppHandle) {
                        if (sparkAppHandle.getState().isFinal()) {
                            countDownLatch.countDown();
                        }
                        System.out.println("state:" + sparkAppHandle.getState().toString());
                    }


                    @Override
                    public void infoChanged(SparkAppHandle sparkAppHandle) {
                        System.out.println("Info:" + sparkAppHandle.getState().toString());
                    }
                });
        System.out.println("The task is executing, please wait ....");
        //线程等待任务结束
        countDownLatch.await();
        System.out.println("The task is finished!");
    }
}
