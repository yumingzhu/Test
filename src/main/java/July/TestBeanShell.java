package July;

import bsh.EvalError;
import bsh.Interpreter;
import org.apache.spark.sql.sources.In;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/7/17 15:34
 */
public class TestBeanShell {
    public static void main(String[] args) throws EvalError {

        long startTime = System.currentTimeMillis();
        Interpreter in = new Interpreter();
        String s = "2>3||3>4&&5<4";
        in.set("boolean", in.eval("(" + s +")"));
        System.out.println(in.get("boolean"));
        System.out.println("耗时:" + (System.currentTimeMillis() - startTime));


    }
}
