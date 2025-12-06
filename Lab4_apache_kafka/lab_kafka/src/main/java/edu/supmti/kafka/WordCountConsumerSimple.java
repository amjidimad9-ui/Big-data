package edu.supmti.kafka;

import java.util.*;
import java.time.Duration;
import org.apache.kafka.clients.consumer.*;

public class WordCountConsumerSimple {
    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Usage: WordCountConsumerSimple <topic>");
            return;
        }
        String topic = args[0];

        Properties props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put("group.id","wordcount-simple");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        Map<String, Long> counts = new HashMap<>();

        while (true) {
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(200));
            for (ConsumerRecord<String,String> rec : records) {
                String word = rec.value();
                counts.put(word, counts.getOrDefault(word, 0L) + 1);
            }
            // Afficher les top 10 mots
            System.out.println("Top mots:");
            counts.entrySet().stream()
                  .sorted((e1,e2)->Long.compare(e2.getValue(), e1.getValue()))
                  .limit(10)
                  .forEach(e -> System.out.println(e.getKey()+ " -> " + e.getValue()));
            System.out.println("----");
        }
    }
}
