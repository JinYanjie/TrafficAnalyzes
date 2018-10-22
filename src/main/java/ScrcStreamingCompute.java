import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.collections.map.HashedMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.lang.reflect.Type;
import java.sql.*;
import java.util.*;

public class ScrcStreamingCompute {
    public static void main(String[] args) throws InterruptedException, SQLException {
        SparkSession sparkSession = SparkSession.builder().appName("bb").master("local[2]").getOrCreate();
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv("traffic.csv");
        dataset.createOrReplaceTempView("t_cltgxx");
        dataset.show(10);
        Dataset<Row> sqlDF = sparkSession.sql("select hphm from t_cltgxx  t where t.tgsj>'2017-06-04 03:03:03'");
        JavaPairRDD<String, String> pairRDD = sqlDF.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                String hphm = row.getAs("hphm");
                return new Tuple2<>(hphm, hphm);
            }
        });

//        kafka 读取每个车辆信息 与全表比对  本来应该使用hive表，hive不能使用，故读取csv文件，创建临时表。
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(javaSparkContext, Durations.seconds(6));

        Connection connection = JdbcUtils.getConnection();
        Statement statement=connection.createStatement();
        String sql="select offset from offset";
        ResultSet resultSet = statement.executeQuery(sql);
        int offset = 1;
        while (resultSet.next()){
            offset=resultSet.getInt("offset");
        }
        JdbcUtils.free(statement,connection);

        Collection<String> topics = new HashSet<>(Arrays.asList("traffic_topic"));
        Collection<TopicPartition> topicPartitions=new HashSet<>(Arrays.asList(new TopicPartition("traffic_topic",1)));

        Map<TopicPartition,Long> topicPartitionLongMap=new HashedMap();
        topicPartitionLongMap.put(new TopicPartition("traffic_topic",1), (long) offset);

        Map<String, Object> kafkaParams = new HashedMap();
        kafkaParams.put("bootstrap.servers", "jyj0.com:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class.getName());
        kafkaParams.put("value.deserializer", StringDeserializer.class.getName());
        kafkaParams.put("group.id", "scrc");

        kafkaParams.put("enable.auto.commit","false");
        kafkaParams.put("auto.offset.reset", "latest");




        JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils.createDirectStream(javaStreamingContext
                , LocationStrategies.PreferConsistent(),
//                ConsumerStrategies.Subscribe(topics, kafkaParams)
                ConsumerStrategies.Assign(topicPartitions,kafkaParams,topicPartitionLongMap)
        );

         directStream.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, String>() {
            @Override
            public Iterator<String> call(ConsumerRecord<String, String> consumerRecord) throws Exception {

                System.out.println("partition:  " + consumerRecord.partition() + "  offset:  " + consumerRecord.offset());
                String jsonData = consumerRecord.value();
                //将json数据转换成 list<String> 返回
                Gson gson = new Gson();
                Type type = new TypeToken<List<Map<String, String>>>() {
                }.getType();
                List<Map<String, String>> mapList = gson.fromJson(jsonData, type);
                List<String> result = new ArrayList<>();
                for (Map<String, String> stringMap : mapList) {
                    StringBuilder stringBuilder = new StringBuilder();
                    stringBuilder.append(stringMap.get("HPHM")).append("_")
                            .append(stringMap.get("CLPP")).append(",")
                            .append(stringMap.get("CLYS")).append(",")
                            .append(stringMap.get("TGSJ")).append(",")
                            .append(stringMap.get("KKBH")).append(",");
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
        })
        .transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaPairRDD<String, String> kafkaPairRDD) throws Exception {
                JavaRDD<String> resultRdd = kafkaPairRDD.leftOuterJoin(pairRDD).filter(new Function<Tuple2<String, Tuple2<String, Optional<String>>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Tuple2<String, Optional<String>>> tuple2) throws Exception {
                        Optional<String> stringOptional = tuple2._2._2;

                        return stringOptional.isPresent();
                    }
                }).map(new Function<Tuple2<String, Tuple2<String, Optional<String>>>, String>() {
                    @Override
                    public String call(Tuple2<String, Tuple2<String, Optional<String>>> t2) throws Exception {
                        System.out.println("t2._1******=" + t2._1);
                        System.out.println("t2._2._1******=" + t2._2._1);
                        System.out.println("t2._2._2******=" + t2._2._2);

                        return t2._1 + "," + t2._2._1;
                    }
                });

                return resultRdd;
            }
        })
        .foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> stringJavaRDD) throws Exception {
                stringJavaRDD.foreachPartition(new VoidFunction<Iterator<String>>() {
                    @Override
                    public void call(Iterator<String> stringIterator) throws Exception {
                        Connection connection = JdbcUtils.getConnection();

//                        connection.setAutoCommit(false);


                        PreparedStatement preparedStatement=null;
                        while (stringIterator.hasNext()){
                            String next = stringIterator.next();
                            String[] fields = next.split(",");
                            String hphm=fields[0];
                            String clpp=fields[1];
                            String clys=fields[2];
                            String tgsj=fields[3];
                            String kkbh=fields[4];
                            String id=String.valueOf(System.currentTimeMillis()).substring(6);
                            String create_time=new Timestamp(System.currentTimeMillis()).toString();
                            String sql="insert into t_bk_tpc (id,hphm,clpp,clys,tgsj,kkbh,create_time) values(?,?,?,?,?,?,?)";
                            preparedStatement=connection.prepareStatement(sql);
                            preparedStatement.setString(1,id);
                            preparedStatement.setString(2,hphm);
                            preparedStatement.setString(3,clpp);
                            preparedStatement.setString(4,clys);
                            preparedStatement.setString(5,tgsj);
                            preparedStatement.setString(6,kkbh);
                            preparedStatement.setString(7,create_time);
                            preparedStatement.executeUpdate();
                        }
//                        connection.commit();
                        JdbcUtils.free(preparedStatement,connection);
                    }
                });
            }
        });

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
        javaStreamingContext.close();


    }
}
