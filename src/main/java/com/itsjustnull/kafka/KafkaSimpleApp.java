package com.itsjustnull.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaSimpleApp {

	public static void main(String[] args) throws IOException {

		KafkaSimpleApp app = new KafkaSimpleApp();
		Person person1 = new Person();

		person1.setPersonId(123);
		person1.setFirstName("John");
		person1.setLastName("Creed");

		Person person2 = new Person();
		person2.setPersonId(456);
		person2.setFirstName("Mark");
		person2.setLastName("Shamon");

		List<Person> personList = new ArrayList<>();

		personList.add(person1);
		personList.add(person2);

		app.writeMessafeToKafka(personList);
		app.readMessageFromKafka();
	}

	public void readMessageFromKafka() {

		Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("group.id", "test-group");

		KafkaConsumer<Integer, String> kafkaConsumer = new KafkaConsumer<Integer, String>(properties);
		List<String> topics = new ArrayList<String>();
		String kafkaTopic = "test";

		topics.add(kafkaTopic);
		kafkaConsumer.subscribe(topics);
		try {
			System.out.println("Kafka consumer reading messages...");

			while (true) {
				ConsumerRecords<Integer, String> records = kafkaConsumer.poll(100);

				for (ConsumerRecord<Integer, String> record : records) {
					System.out
							.println("Kafka message: " + " Topic - " + record.topic() + " Message: " + record.value());
				}
			}

		} catch (Exception e) {

			System.out.println(e.getMessage());
		} finally {
			kafkaConsumer.close();
		}
	}

	public void writeMessafeToKafka(List<Person> personList) {
		Properties properties = new Properties();

		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<Integer, String>(properties);

		String kafkaTopic = "test";

		try {
			System.out.println("Kafka producer sending messages...");
			for (int i = 0; i < personList.size(); i++) {

				kafkaProducer.send(new ProducerRecord<Integer, String>(kafkaTopic, personList.get(i).getPersonId(),
						personList.get(i).toString()));
				System.out.println("Kafka message:~ " + " Topic - " + kafkaTopic + " Key: "
						+ personList.get(i).getPersonId() + " Message: " + personList.get(i).toString());

			}
			System.out.println("Kafka producer done sending messages...");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			kafkaProducer.close();
		}
	}
}
