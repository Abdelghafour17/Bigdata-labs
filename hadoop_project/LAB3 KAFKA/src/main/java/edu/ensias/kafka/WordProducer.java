package edu.ensias.kafka;

import java.util.Properties;
import java.util.Scanner;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class WordProducer {
    public static void main(String[] args) {

        if (args.length == 0 || args.length > 1) {
            System.out.println("Usage: java -jar <jar_name> <topic_name>");
            return;
        }

        String topicName = args[0];
        Properties props = new Properties();
        
        // Configuration pour se connecter à n'importe quel broker (ici le broker 0)
        props.put("bootstrap.servers", "localhost:9092"); 
        
        props.put("acks", "all");
        props.put("retries", 0);
        
        // Sérialiseurs (clé et valeur sont des String)
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (Producer<String, String> producer = new KafkaProducer<>(props);
             Scanner scanner = new Scanner(System.in)) {

            System.out.println("Producteur WordCount démarré. Topic: " + topicName);
            System.out.println("Entrez du texte (une ligne par envoi). Tapez 'exit' pour quitter:");

            while (true) {
                String line = scanner.nextLine();
                if (line.equalsIgnoreCase("exit")) {
                    break;
                }
                
                // Diviser la ligne en mots (l'apostrophe sera traitée comme séparateur)
                String[] words = line.toLowerCase().split("\\s+|\\W+");

                for (String word : words) {
                    if (!word.isEmpty()) {
                        // Envoyer le mot (sans clé pour l'instant)
                        producer.send(new ProducerRecord<>(topicName, null, word)); 
                        // Note: le deuxième argument (null) est la clé. Si null, Kafka l'envoie de manière round-robin
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}