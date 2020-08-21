package Dec.function;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

public class StudentComparator  implements Comparator<String>,Serializable {

    @Override
    public int compare(String o1 , String o2) {
        String[] o1_split = o1.split("_");
        String[] o2_split = o2.split("_");
        Double o1_w = Double.valueOf(o1_split[0]);
        Double o1_s = Double.valueOf(o1_split[1]);
        Double o2_w = Double.valueOf(o2_split[0]);
        Double o2_s = Double.valueOf(o2_split[1]);
        if (o1_w > o2_w) {
            return -1;
        } else if (o1_w == o2_w) {
            if (o1_s >= o2_s) {
                return -1;
            }
        } else {
            return 1;
        }
        return 0;
    }
}
