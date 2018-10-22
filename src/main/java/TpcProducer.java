import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;

public class TpcProducer {
    public static void main(String[] args) {
        Properties properties=new Properties();
        properties.put("bootstrap.servers","jyj0.com:9092");
        properties.put("key.serializer",StringSerializer.class.getName());
        properties.put("value.serializer",StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        Gson gson=new Gson();

        List<Map<String,String>> message=new ArrayList<>();
        for(int i=0;i<1;i++){
            Map<String,String> data=new HashMap<String, String>();
            data.put("HPHM", "粤C26GP4");
            data.put("CLPP", "丰田");
            data.put("CLYS", "蓝");
            data.put("TGSJ", "2018-09-26 17:35:00");
            data.put("KKBH","798798594");
            message.add(data);
        }
        String json = gson.toJson(message);
        System.out.println(json);

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("traffic_topic", json);

        try {
            kafkaProducer.send(producerRecord);
            kafkaProducer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
