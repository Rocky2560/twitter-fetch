package kafka.twitter;


import com.google.gson.JsonObject;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class KakfaProducerConfig {
    Logger logger = LoggerFactory.getLogger(KakfaProducerConfig.class.getName());

    public KafkaProducer<String, String> createKafkaProducer() throws IOException {

        GetProperty gp = new GetProperty();

        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, gp.getBootstrapServer());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        return producer;
    }


    public void SendToTopic(String topic, KafkaProducer<String, String> producer, JsonObject info) {
        producer.send(new ProducerRecord<>(topic, null, info.toString()), new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e != null) {
                    System.out.println(e);
                    logger.error("User Producer Exception", e);
                }
            }
        });
        producer.flush();
    }
}
