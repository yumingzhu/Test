package July;

import bsh.Interpreter;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.util.ArrayList;
import java.util.List;

/**
 * @Description 将string中的内容当作java语句执行
 * @Author zhangjw
 * @Date 2019/7/11 10:38
 */
public class EvalUtils {

	private static Interpreter interpreter = new Interpreter();

	private static ScriptEngineManager scriptEngineManager = new ScriptEngineManager();

	private static ScriptEngine scriptEngine = scriptEngineManager.getEngineByName("js");

	public static void main(String[] args) {

		//		List<String> list = new ArrayList<>();
		//		list.add("(true||false)&&true&&(false||true)");
		//		list.add("(true||false)");
		//		list.add("true&&(false||true)");
		//		list.add("(true||false)&&true");
		//		list.add("(false||true)");
		//		list.add("(true||false)&&(false||true)");
		//		list.add("(true||false)&&false");
		//		list.add("(true||false)&&true&&false");
		//		list.add("(true||false)&&true&&true");
		//		list.add("(true||false)&&(true||true)");
		//		list.add("(true||true)&&true&&(false||true)");
		//		iter
		String value3 = "true&true";

		long startTime2 = System.currentTimeMillis();
		boolean operate4 = EvalUtils.operate2(value3);
		System.out.println("operate2 = " + operate4);
		System.out.println("耗时:" + (System.currentTimeMillis() - startTime2));

	}

	/**
	 * 执行string中的语句
	 * @return
	 */
	public static boolean operate1(String value) {
		try {
			return (Boolean) interpreter.eval(value);
		} catch (Exception e) {
			System.out.println("EvalUtils.operate执行报错，原因为：" + e.getMessage());
			return false;
		}
	}

	/**
	 * 执行string中的语句
	 * @return
	 */
	public static boolean operate2(String value) {
		try {
			boolean result = (Boolean) scriptEngine.eval(value);
			return result;
		} catch (Exception e) {
			System.out.println("EvalUtils.operate执行报错，原因为：" + e.getMessage());
			return false;
		}
	}
}
