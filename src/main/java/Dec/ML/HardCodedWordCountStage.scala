//package Dec.ML
//
//import breeze.linalg.*
//import org.apache.spark.ml.Transformer
//import org.apache.spark.ml.param.ParamMap
//import org.apache.spark.ml.util.Identifiable
//import org.apache.spark.sql.{DataFrame, Dataset}
//import org.apache.spark.sql.types.{StringType, StructType}
//
//class HardCodedWordCountStage(override val uid: String) extends Transformer {
//
//  def this() = this(Identifiable.randomUID(
//
//  "hardcodedwordcount" ) )
//
//  def copy(extra: ParamMap): HardCodedWordCountStage = {
//
//    defaultCopy(extra)
//
//  }
//
//  override def transform(dataset: Dataset[_]): DataFrame =  {
//
//    // Check that the input type is a string
//
//    val idx = schema.fieldIndex("happy_pandas")
//
//    val field = schema.fields(idx)
//
//    if (field.dataType != StringType) {
//
//      throw new Exception(s"Input type ${field.dataType} did not match input type StringType")
//
//    }
//
//
//   override def transformSchema(schema: StructType): StructType = {
//      val wordcount = udf { in: String => in.split(" ").size }
//
//      df.select(col("*"),
//
//      wordcount(df.col("happy_pandas")).as("happy_panda_counts"))
//    }
//}