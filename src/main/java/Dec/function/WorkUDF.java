package Dec.function;

import org.apache.spark.sql.api.java.UDF1;

public class WorkUDF  implements UDF1<String,String> {
    @Override
    public String call(String s) throws Exception {
        return s.toUpperCase();
    }
}
