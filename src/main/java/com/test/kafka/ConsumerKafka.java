package com.test.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerKafka {
	private static final Logger log = LoggerFactory.getLogger(ConsumerKafka.class);
	public static void main(String[] args) {
		
		String groupId="my kafka app";
		
		Properties prop=new Properties();
		//connect to Localhost
		//prop.setProperty("bootstrap.servers", "127.0.0.1:9092");
		String topic="test_topic";
		prop.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
		prop.setProperty("security.protocol", "SASL_SSL");
		prop.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"13JhmxXXubqKWX2CEAMNXt\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIxM0pobXhYWHVicUtXWDJDRUFNTlh0Iiwib3JnYW5pemF0aW9uSWQiOjczMTk2LCJ1c2VySWQiOjg1MDg2LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiJhZTgzNmEwZS05YmU4LTQwZmEtOWFlNi1iYmEwZDRkNDM0MjIifX0.6AEuwe2V_ZhEKSOCzkpQhuZwTFcGPV1vcGBSNwBEKlk\";");
		prop.setProperty("sasl.mechanism", "PLAIN");
		
		prop.setProperty("key.deserializer", StringDeserializer.class.getName());
		prop.setProperty("value.deserializer", StringDeserializer.class.getName());
		
		prop.setProperty("group.id", groupId);
		prop.setProperty("auto.offset.reset", "earliest");
		
		KafkaConsumer<String, String> consumer=new KafkaConsumer<>(prop);
		//subscribe to a topic
		consumer.subscribe(Arrays.asList(topic));
		while(true)
		{
			log.info("polling");
			ConsumerRecords<String, String> records=consumer.poll(Duration.ofMillis(1000));
			for(ConsumerRecord<String, String> record: records)
			{
				log.info("Key: "+record.key()+", Value: "+record.value());
				log.info("Partision: "+record.partition()+", Offset: "+record.offset());
			}
		}

	}

}
