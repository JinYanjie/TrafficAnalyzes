import org.apache.kafka.common.protocol.types.Field;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.CollectionAccumulator;
import scala.Tuple2;

import java.sql.Timestamp;
import java.util.Date;
import java.util.Iterator;
import java.util.List;


/**
 * 套牌车分析算法实现
 * brave
 */
public class TpcCompute {
    public static void main(String[] args) {
//        计算套牌车  hdfs获取数据
        SparkSession sparkSession = SparkSession.builder().master("local").appName("aa").getOrCreate();
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        javaSparkContext.setLogLevel("ERROR");

        Dataset<Row> csv = sparkSession.read().option("header", "true").csv("hdfs://jyj0.com:8020/traffic/traffic.csv");
        csv.show(10);
        csv.printSchema();

        csv.createOrReplaceTempView("t_traffic");
        Dataset<Row> filterDF = sparkSession.sql("select * from t_traffic t where t.tgsj>'2017-03-09 04:04:04'");
        filterDF.show(10);

//        将同一车牌号 经过的卡口聚到一起  需要pairRdd
        JavaPairRDD<String, String> pairRDD = filterDF.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                String ID = row.getAs("ID");
                String HPHM = row.getAs("HPHM");
                String TGSJ = row.getAs("TGSJ");
                String KKBH = row.getAs("KKBH");
                String KK_LON_LAT = row.getAs("KK_LON_LAT");

                return new Tuple2<>(HPHM, ID + "_" + TGSJ + "_" + KKBH + "_" + KK_LON_LAT);
            }
        });

//        reduceByKey 将相同的牌号 车辆整合
        JavaPairRDD<String, String> reduceRdd = pairRDD.reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String v1, String v2) throws Exception {

                return v1 + "&" + v2;
            }
        });

        CollectionAccumulator<String> collectionAccumulator = javaSparkContext.sc().collectionAccumulator();
        reduceRdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {
                while (tuple2Iterator.hasNext()) {
                    Tuple2<String, String> tuple2 = tuple2Iterator.next();
                    String[] strings = tuple2._2.split("&");
                    if (strings.length > 1) {
                        for (int i = 0; i < strings.length; i++) {
                            for (int k = i + 1; k < strings.length; k++) {
                                String info1 = strings[i];
                                String info2 = strings[k];

                                String[] value1 = info1.split("_");
                                String id = value1[0];
                                String tgsj = value1[1];
                                String kkbh = value1[2];
                                String lon = value1[3];
                                String lat = value1[4];

                                String[] value2 = info2.split("_");
                                String id2 = value2[0];
                                String tgsj2 = value2[1];
                                String kkbh2 = value2[2];
                                String lon2 = value2[3];
                                String lat2 = value2[4];
                                double subHour = TestDataUtils.getSubHour(tgsj, tgsj2);
                                double longDistance = TestDataUtils.getLongDistance(Double.valueOf(lon), Double.valueOf(lat), Double.valueOf(lon2), Double.valueOf(lat2));
                                Integer speed = TestDataUtils.getSpeed(longDistance, subHour);
                                if (speed > 180) {
                                    collectionAccumulator.add(id + "_" + id2);
                                }

                            }
                        }
                    }
                }
            }
        });

        List<String> lists = collectionAccumulator.value();
        for (String list : lists) {
            Dataset<Row> rowDataset = sparkSession.sql("select id,hphm,clpp,clys,tgsj,kkbh from t_traffic where id in (" + list.split("_")[0] + "," + list.split("_")[1] + ")");
            Dataset<Row> newDF = rowDataset.withColumn("jsbh", functions.lit(new Date().getTime()))
                    .withColumn("create_time", functions.lit(new Timestamp(new Date().getTime())));

            newDF.show();

            newDF.write()
                    .format("jdbc")
                    .option("url","jdbc:mysql://jyj0.com:3306/traffic?characterEncoding=UTF-8")
                    .option("dbtable","t_tpc_result")
                    .option("user","root")
                    .option("password","123456")
                    .mode(SaveMode.Append)
                    .save();
        }

    }

}
