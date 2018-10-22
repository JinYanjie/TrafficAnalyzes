import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.collections.map.HashedMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.LocationStrategy;
import scala.Tuple2;

import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.*;

public class TpcStreamingCompute {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[2]").appName("aa").getOrCreate();
        Dataset<Row> rowDataset = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://jyj0.com:3306/traffic?characterEncoding=UTF-8")
                .option("user", "root")
                .option("password", "123456")
                .option("dbtable", "t_tpc_result")
                .load();
        rowDataset.show();

        Dataset<Row> hphmDF = rowDataset.select("hphm").distinct();
        hphmDF.show();


        hphmDF.cache();
        JavaRDD<Row> tpcRdd = hphmDF.javaRDD();

        JavaPairRDD<String, String> hphmRdd = tpcRdd.mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                String hphm = row.getAs("hphm");
                return new Tuple2<>(hphm, hphm);
            }
        });

        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
        String brokers = "jyj0.com:9092";
        String topic = "traffic_topic";
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(javaSparkContext, Durations.seconds(6));

        Collection<String> topics = new HashSet<>(Arrays.asList(topic.split(",")));
        Map<String, Object> kafkaParames = new HashMap<>();
        kafkaParames.put("bootstrap.servers", brokers);
        kafkaParames.put("group.id", "tpc_group");
        kafkaParames.put("auto.offset.reset", "latest");
        kafkaParames.put("key.deserializer", StringDeserializer.class.getName());
        kafkaParames.put("value.deserializer", StringDeserializer.class.getName());

        Map<TopicPartition, Long> offsets = new HashedMap();

        JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils.createDirectStream(javaStreamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParames, offsets));

        JavaPairDStream<String, String> pairDStream = directStream.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, String>() {
            @Override
            public Iterator<String> call(ConsumerRecord<String, String> stringStringConsumerRecord) throws Exception {
                List<String> result = new ArrayList<>();
                String value = stringStringConsumerRecord.value();
                System.out.println("kafka 数据流" + value);
                Gson gson = new Gson();
                Type type = new TypeToken<List<Map<String, String>>>() {
                }.getType();
                List<Map<String, String>> mapList = gson.fromJson(value, type);

                for (Map<String, String> stringStringMap : mapList) {
                    StringBuilder stringBuilder = new StringBuilder();
                    stringBuilder.append(stringStringMap.get("HPHM")).append("_")
                            .append(stringStringMap.get("CLPP")).append(",")
                            .append(stringStringMap.get("CLYS")).append(",")
                            .append(stringStringMap.get("TGSJ")).append(",")
                            .append(stringStringMap.get("KKBH")).append(",");
                    result.add(stringBuilder.toString());
                }
                return result.iterator();
            }
        })
                .mapToPair(new PairFunction<String, String, String>() {
                    @Override
                    public Tuple2<String, String> call(String s) throws Exception {
                        return new Tuple2<>(s.split("_")[0], s.split("_")[1]);
                    }
                });
        pairDStream.print();
        JavaDStream<String> resultDstream = pairDStream.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaPairRDD<String, String> pairRDD) throws Exception {
                JavaRDD<String> resultRdd = pairRDD.join(hphmRdd).map(new Function<Tuple2<String, Tuple2<String, String>>, String>() {
                    @Override
                    public String call(Tuple2<String, Tuple2<String, String>> v1) throws Exception {

                        return v1._1 + "," + v1._2._1;
                    }
                });
                return resultRdd;
            }
        });

        resultDstream.print();

        resultDstream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> stringJavaRDD) throws Exception {
                stringJavaRDD.foreachPartition(new VoidFunction<Iterator<String>>() {
                    @Override
                    public void call(Iterator<String> stringIterator) throws Exception {
                        Connection connection = JdbcUtils.getConnection();
                        PreparedStatement preparedStatement = null;
                        while (stringIterator.hasNext()) {
                            String next = stringIterator.next();

                            String[] fields = next.split(",");
                            String hphm = fields[0];
                            String clpp = fields[1];
                            String clys = fields[2];
                            String tgsj = fields[3];
                            String kkbh = fields[4];
                            String id = String.valueOf(System.currentTimeMillis()).substring(6);
                            String create_time = new Timestamp(System.currentTimeMillis()).toString();
                            String sql = "insert into t_bk_tpc (id,hphm,clpp,clys,tgsj,kkbh,create_time) values (?,?,?,?,?,?,?)";
                            preparedStatement = connection.prepareStatement(sql);
                            preparedStatement.setString(1,id);
                            preparedStatement.setString(2,hphm);
                            preparedStatement.setString(3,clpp);
                            preparedStatement.setString(4,clys);
                            preparedStatement.setString(5,tgsj);
                            preparedStatement.setString(6,kkbh);
                            preparedStatement.setString(7,create_time);
                            preparedStatement.executeUpdate();
                        }
                        JdbcUtils.free(preparedStatement,connection);

                    }
                });

            }
//                JavaRDD<Row> rowJavaRDD = stringJavaRDD.map(new Function<String, Row>() {
//                    @Override
//                    public Row call(String v1) throws Exception {
//                        String[] fields = v1.split(",");
//                        String hphm = fields[0];
//                        String clpp = fields[1];
//                        String clys = fields[2];
//                        String tgsj = fields[3];
//                        String kkbh = fields[4];
//                        return RowFactory.create(String.valueOf(System.currentTimeMillis()).substring(6),hphm, clpp, clys, tgsj, kkbh,new Timestamp(System.currentTimeMillis()).toString());
//                    }
//                });
//                StructField[] fields=new StructField[]{
//                        DataTypes.createStructField("id", DataTypes.StringType, false),
//                        DataTypes.createStructField("hphm", DataTypes.StringType, false),
//                        DataTypes.createStructField("clpp", DataTypes.StringType, false),
//                        DataTypes.createStructField("clys", DataTypes.StringType, false),
//                        DataTypes.createStructField("tgsj", DataTypes.StringType, false),
//                        DataTypes.createStructField("kkbh", DataTypes.StringType, false),
//                        DataTypes.createStructField("create_time", DataTypes.StringType, false)
//                };
//                Dataset<Row> dataFrame = spark.createDataFrame(rowJavaRDD, DataTypes.createStructType(fields));
//                dataFrame.show();
//                dataFrame.write()
//                        .format("jdbc")
//                        .option("url","jdbc:mysql://jyj0.com:3306/traffic?characterEncoding=UTF-8")
//                        .option("dbtable","t_bk_tpc")
//                        .option("user","root")
//                        .option("password","123456")
//                        .mode(SaveMode.Append)
//                        .save();
//
//            }
        });

        try {
            javaStreamingContext.start();
            javaStreamingContext.awaitTermination();
            javaStreamingContext.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
