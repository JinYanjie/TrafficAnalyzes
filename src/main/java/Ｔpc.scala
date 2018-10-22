import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Ｔpc {

  case class CLXX(id: String, hphm: String, hpzl: String, hpys: String, clpp: String, clys: String, tgsj: String, kkbh: String, tpdz: String, kkwz: String)

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local").appName("aa")
      .getOrCreate()
    val frame = sparkSession.read
      .option("header", "true")
      .csv("hdfs://jyj0.com:8020/traffic/traffic.csv")
    frame.printSchema()

    import sparkSession.implicits._
//    val dataset = frame.as[CLXX]
//    dataset.printSchema()
//    dataset.show(10)
    frame.createOrReplaceTempView("clxx")
    val selectFrame = sparkSession.sql("select * from clxx t where t.tgsl>'2017-10-25 03:06:00'")
    selectFrame.show(10)


  }
}
