import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.*;

public class TxcCompute {
    public static void main(String[] args) {

//        1.	如果两辆车都通过相同序列的卡口(卡口编号)。
//        2.	通过同一卡口之间的时间差小于3分钟。

//        相同序列的卡口编号作为key,
//

        SparkSession spark = SparkSession.builder().appName("aa").master("local").getOrCreate();
        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
        javaSparkContext.setLogLevel("ERROR");


        Dataset<Row> csv = spark.read().option("header", "true").csv("hdfs://jyj0.com:8020/traffic/traffic.csv");
//        转换成rdd
        csv.show(10);
        csv.createOrReplaceTempView("t_traffic");
        Dataset<Row> sqlDF = spark.sql("select * from t_traffic t where t.tgsj>'2017-03-03 03:03:03'");

        /**
         * kkbh1,kkbh2,kkbh3,:(hphm1,[t1,t2,t3,])
         * kkbh2,kkbh3,kkbh4,:(hphm1,[t2,t3,t4,])
         * kkbh3,kkbh4,kkbh5,:(hphm1,[t3,t4,t5,])
         */

        JavaPairRDD<String, Tuple2<String, String>> pairRDD = sqlDF.toJavaRDD().mapToPair(new PairFunction<Row, String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, Tuple2<String, String>> call(Row row) throws Exception {
                String hphm = row.getAs("HPHM");
                String kkbh = row.getAs("KKBH");
                String tgsj = row.getAs("TGSJ");
                return new Tuple2<>(hphm, new Tuple2<>(kkbh, tgsj));
            }
        });

        /**
         * kkbh1,kkbh2,kkbh3,:(hphm1,[t1,t2,t3,])
         * kkbh2,kkbh3,kkbh4,:(hphm1,[t2,t3,t4,])
         * kkbh3,kkbh4,kkbh5,:(hphm1,[t3,t4,t5,])
         */

        JavaPairRDD<String, Iterable<Tuple2<String, String>>> groupRdd = pairRDD.groupByKey();
        JavaRDD<Tuple2<String, Tuple2<String, String>>> tuple2JavaRDD = groupRdd.flatMap(new FlatMapFunction<Tuple2<String, Iterable<Tuple2<String, String>>>, Tuple2<String, Tuple2<String, String>>>() {
            @Override
            public Iterator<Tuple2<String, Tuple2<String, String>>> call(Tuple2<String, Iterable<Tuple2<String, String>>> stringIterableTuple2) throws Exception {
                String hphm = stringIterableTuple2._1;
                List<Tuple2<String, Tuple2<String, String>>> result = new ArrayList<Tuple2<String, Tuple2<String, String>>>();
                Iterable<Tuple2<String, String>> tuple2s = stringIterableTuple2._2;
                List<Tuple2<String, String>> list = IteratorUtils.toList(tuple2s.iterator());

                for (int i = 0; i < list.size() - 2; i++) {
                    StringBuilder sbTime = new StringBuilder();
                    StringBuilder sbKkbh = new StringBuilder();

                    for (int j = i; j < i + 3; j++) {
                        sbTime.append(list.get(j)._2).append(",");
                        sbKkbh.append(list.get(j)._1).append(",");
                    }
                    System.out.println("sbTime:" + sbTime.toString());
                    System.out.println("sbKkbh:" + sbKkbh.toString());
                    result.add(new Tuple2<>(sbKkbh.toString(), new Tuple2<String, String>(hphm, sbTime.toString())));
                }

                return result.iterator();
            }
        });

        tuple2JavaRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<String, String>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<String, String>> stringTuple2Tuple2) throws Exception {
                System.out.println(stringTuple2Tuple2._1 + ":(" + stringTuple2Tuple2._2._1 + ",[" + stringTuple2Tuple2._2._2 + "])");
            }
        });

        JavaRDD<String> filter = tuple2JavaRDD.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, String>>, String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, Tuple2<String, String>> call(Tuple2<String, Tuple2<String, String>> stringTuple2Tuple2) throws Exception {
                return new Tuple2<>(stringTuple2Tuple2._1, stringTuple2Tuple2._2);
            }
        })
                //相同3个的卡口号 分到一组
                .groupByKey()
//                map将每一组里面的遍历 目的是为了算时间
                .map(new Function<Tuple2<String, Iterable<Tuple2<String, String>>>, String>() {
                    @Override
                    public String call(Tuple2<String, Iterable<Tuple2<String, String>>> v1) throws Exception {
                        Set<String> hphmSet = new HashSet<String>();
                        StringBuilder sbHphm = new StringBuilder();
                        List<Tuple2<String, String>> list = IteratorUtils.toList(v1._2.iterator());
                        for (int i = 0; i < list.size(); i++) {
                            for (int k = i + 1; k < list.size(); k++) {

                                String[] time1 = list.get(i)._2.split(",");
                                String[] time2 = list.get(k)._2.split(",");
                                for (int m = 0; m < time1.length; m++) {
                                    double subminutes = TestDataUtils.getSubminutes(time1[m], time2[m]);
                                    if (subminutes <= 3) {
                                        hphmSet.add(list.get(i)._1);
                                        hphmSet.add(list.get(k)._1);
                                    }
                                }

                            }
                        }
                        for (String s : hphmSet) {
                            sbHphm.append(s).append(",");
                        }

                        return v1._1 + "&" + sbHphm.toString();
                    }
                })
                .filter(new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String v1) throws Exception {
                        return v1.split("&").length > 1;
                    }
                });
        JavaRDD<Row> map = filter.map(new Function<String, Row>() {
            @Override
            public Row call(String v1) throws Exception {
                String HPHM = v1.split("&")[1];
                String KKBH = v1.split("&")[0];
                String JSBH =String.valueOf(System.currentTimeMillis());
                String ID = String.valueOf(JSBH).substring(6);
                String CREATE_TIME = new Timestamp(System.currentTimeMillis()).toString();
                return RowFactory.create(
                        ID,
                        JSBH,
                        HPHM,
                        KKBH,
                        CREATE_TIME
                );
            }
        });
        StructField[] fields=new StructField[]{
                DataTypes.createStructField("ID", DataTypes.StringType, false),
                DataTypes.createStructField("JSBH", DataTypes.StringType, false),
                DataTypes.createStructField("HPHM", DataTypes.StringType, false),
                DataTypes.createStructField("KKBH", DataTypes.StringType, false),
                DataTypes.createStructField("CREATE_TIME", DataTypes.StringType, false)
        };
        Dataset<Row> dataFrame= spark.createDataFrame(map, DataTypes.createStructType(fields));
        dataFrame.show();
        dataFrame.write()
                .format("jdbc")
                .option("url","jdbc:mysql://jyj0.com:3306/traffic?characterEncoding=UTF-8")
                .option("dbtable","t_txc_result")
                .option("user","root")
                .option("password","123456")
                .mode(SaveMode.Append)
                .save();
//                .foreachPartition(new VoidFunction<Iterator<String>>() {
//                    @Override
//                    public void call(Iterator<String> stringIterator) throws Exception {
//                        List<String> list = IteratorUtils.toList(stringIterator);
//                        Connection conn = JdbcUtils.getConnection();
//                        PreparedStatement preparedStatement = null;
//                        for (String s : list) {
//                            String hphm = s.split("&")[1];
//                            String kkbh = s.split("&")[0];
//                            System.out.println(kkbh + " ：" + hphm);
//
//                            String sql = "insert into t_txc_result (ID,JSBH,HPHM,KKBH,CREATE_TIME) values(?,?,?,?,?)";
//                            preparedStatement = conn.prepareStatement(sql);
//                            long jsbh = System.currentTimeMillis();
//                            preparedStatement.setString(1, String.valueOf(jsbh).substring(6));
//                            preparedStatement.setString(2, jsbh + "");
//                            preparedStatement.setString(3, hphm);
//                            preparedStatement.setString(4, kkbh);
//                            preparedStatement.setTimestamp(5, new Timestamp(jsbh));
//                            preparedStatement.execute();
//                        }
//                        JdbcUtils.free(preparedStatement, conn);
//                    }
//                });

    }
}
