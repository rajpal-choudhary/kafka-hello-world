package producers;

import lib.GetProperties;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

class HelloProducer {

    public static void main(String[] args) {
        Properties props = GetProperties.getConfluentCloudProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Batching
        // props.put(ProducerConfig.BATCH_SIZE_CONFIG, 200);

        // Ack config (all -1, leader 1, none=0)
        // props.put(ProducerConfig.ACKS_CONFIG, "all");

        // Transactional producer
        // props.put("transactional.id", "transactional-id");

        KafkaProducer<String, String> producer = new KafkaProducer(props);
        HelloProducer hp = new HelloProducer();

        // Transactional producer
        // producer.initTransactions();

        // Fire and forget method
        // hp.fireNForget(producer, props.getProperty("HELLO_TOPIC"));

        // Sync message
        // hp.syncMsg(producer, props.getProperty("HELLO_TOPIC"));

        // ASync message
        // hp.asyncMsg(producer, props.getProperty("HELLO_TOPIC"));

        producer.close();
    }

    public void fireNForget(KafkaProducer producer, String Topic) {

        IntStream.range(0, 100).forEach(v -> {
            ProducerRecord<String, String> pr = new ProducerRecord(Topic, v + "", "Value-" + v);
            producer.send(pr);
        });
    }

    public void syncMsg(KafkaProducer producer, String Topic) {
        // Transactional producer
        // producer.beginTransaction();

        IntStream.range(0, 100).forEach(v -> {
            ProducerRecord<String, String> pr = new ProducerRecord(Topic, v + "", "Value-" + v);
            try {
                RecordMetadata rm = (RecordMetadata) producer.send(pr).get();
                System.out.println(rm.toString());
            } catch (InterruptedException e) {
                // Transactional producer
                // producer.abortTransaction();
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            System.out.println("Value =>" + v);
        });

        // Transactional producer
        // producer.commitTransaction();
    }


    public void asyncMsg(KafkaProducer producer, String Topic) {
        IntStream.range(0, 100).forEach(v -> {
            ProducerRecord<String, String> pr = new ProducerRecord(Topic, v + "", "Value-" + v);
            producer.send(pr, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        e.printStackTrace();
                    } else {
                        System.out.println("The offset of the record we just sent is: " + recordMetadata.offset());
                    }
                }
            });

            System.out.println("Value =>" + v);
        });
    }
}