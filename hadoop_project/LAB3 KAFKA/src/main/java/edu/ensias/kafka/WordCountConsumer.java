package edu.ensias.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class WordCountConsumer {
    public static void main(String[] args) {

        if (args.length == 0 || args.length > 1) {
            System.out.println("Usage: java -jar <jar_name> <topic_name>");
            return;
        }

        String topicName = args[0];
        Properties props = new Properties();

        // Configuration du broker
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094"); 
        
        // Configuration du groupe et du reset d'offset (pour lire depuis le début)
        props.put("group.id", "wordcount-group");
        props.put("auto.offset.reset", "earliest");
        
        // Désérialiseurs
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // Map pour stocker la fréquence des mots (notre "base de données" en mémoire)
        Map<String, Integer> wordCounts = new HashMap<>();

        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            
            // Abonnement au Topic WordCount
            consumer.subscribe(Arrays.asList(topicName));
            System.out.println("Consommateur WordCount démarré. Abonné au topic " + topicName);
            System.out.println("\n--- Fréquence des Mots en Temps Réel ---\n");

            while (true) {
                // Poll: demande de nouveaux messages au broker
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                
                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, String> record : records) {
                        String word = record.value();
                        
                        // Logique de WordCount: Incrémenter la fréquence du mot
                        wordCounts.put(word, wordCounts.getOrDefault(word, 0) + 1);
                    }
                    
                    // Afficher la fréquence mise à jour après chaque lot de messages
                    System.out.println("----------------------------------------");
                    wordCounts.forEach((word, count) -> 
                        System.out.printf("| %-15s | %4d |\n", word, count)
                    );
                    System.out.println("----------------------------------------");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}