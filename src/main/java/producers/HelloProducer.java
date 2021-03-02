package producers;

import lib.GetProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

class HelloProducer {

    public static void main(String[] args) {
        Properties props = GetProperties.getConfluentCloudProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer<String, String> producer = new KafkaProducer(props);
        HelloProducer hp = new HelloProducer();
        hp.fireNForget(producer, props.getProperty("HELLO_TOPIC"));

        producer.close();
    }

    public void fireNForget(KafkaProducer producer, String Topic) {
        IntStream.range(0, 100).forEach(v -> {
            ProducerRecord<String, String> pr = new ProducerRecord(Topic, v + "", "Value-" + v);
            try {
                RecordMetadata rm = (RecordMetadata) producer.send(pr).get();
                System.out.println(rm.toString());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            System.out.println("Value =>" + v);
        });
    }
}