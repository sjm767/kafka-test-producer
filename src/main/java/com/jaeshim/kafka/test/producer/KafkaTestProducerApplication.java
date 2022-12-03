package com.jaeshim.kafka.test.producer;

import com.jaeshim.kafka.test.producer.partitioner.RackAwarenessRoundRobinPartitioner;
import java.util.Properties;
import java.util.Scanner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.clients.producer.UniformStickyPartitioner;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaTestProducerApplication {

	private static final String FIN_MESSAGE = "exit";

	private static final String TOPIC = "jaeshim-20221121";

	public static void main(String[] args) {
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092, localhost:9093");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DefaultPartitioner.class);
		
		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

		while(true) {
			Scanner sc = new Scanner(System.in);
			System.out.print("Input > ");
			String message = sc.nextLine();

			if(FIN_MESSAGE.equals(message)) {
				producer.close();
				break;
			}

			ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, message);
			try {
				producer.send(record, (metadata, exception) -> {
					if (exception != null) {
						// some exception
					}
				});

			} catch (Exception e) {
				// exception
			} finally {
				producer.flush();
			}


		}
	}

}
