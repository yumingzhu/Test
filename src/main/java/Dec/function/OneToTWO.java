package Dec.function;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class OneToTWO implements PairFlatMapFunction<String,String,Integer> {
    @Override
    public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
        List<Tuple2<String,Integer>> list=new ArrayList<>();
        String[] split = s.split(" ");


        for (String  s1:s.split(" ")) {
                    list.add(new Tuple2<String, Integer>(s1,1));
                 }
                return list.iterator();
    }
}
