package javaTest;

import org.apache.spark.sql.api.java.UDF14;
import org.apache.spark.sql.api.java.UDF16;

/**
 * @Description 余弦相似度UDF
 * @Author yumigzhu
 * @Date 2019/1/14 14:58
 * 
 */
public class CosUDF   implements UDF14<Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double,Double> {
    @Override
    public Double call(Double a1, Double a2, Double a3, Double a4, Double a5, Double a6, Double a7, Double b1, Double b2, Double b3, Double b4, Double b5, Double b6, Double b7) throws Exception {
         Double molecule=a1*b1+a2*b2+a3*b3+a4*b4+a5*b5+a6*b6+a7*b7;  //分子
         Double a= Math.sqrt(Math.pow(a1,2)+Math.pow(a2,2)+Math.pow(a3,2)+Math.pow(a4,2)+Math.pow(a5,2)+Math.pow(a6,2)+Math.pow(a7,2));
         Double b= Math.sqrt(Math.pow(b1,2)+Math.pow(b2,2)+Math.pow(b3,2)+Math.pow(b4,2)+Math.pow(b5,2)+Math.pow(b6,2)+Math.pow(b7,2));
         Double  cosa_b=molecule/(a*b);
        return cosa_b;
    }


}
