package edu.supmti.kafka;

import java.util.Properties;
import java.util.Scanner;
import org.apache.kafka.clients.producer.*;

public class WordProducer {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Usage: WordProducer <topic>");
            return;
        }
        String topic = args[0];

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        Producer<String,String> producer = new KafkaProducer<>(props);
        Scanner sc = new Scanner(System.in);
        System.out.println("Entrez des mots (ou 'exit' pour quitter) :");

        while (sc.hasNextLine()) {
            String line = sc.nextLine();
            if ("exit".equalsIgnoreCase(line)) break;
            String[] words = line.split("\\s+");
            for (String w : words) {
                producer.send(new ProducerRecord<>(topic, w, w));
            }
        }
        producer.close();
        sc.close();
    }
}