package com.test.kafka;



import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerKafka {
	 private static final Logger log = LoggerFactory.getLogger(ProducerKafka.class);
	public static void main(String[] args) {
		// TODO Auto-generated method stub
log.info("Welcome to KAFKA");
//create producer property
Properties prop=new Properties();
//connect to Localhost
//prop.setProperty("bootstrap.servers", "127.0.0.1:9092");

prop.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
prop.setProperty("security.protocol", "SASL_SSL");
prop.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"13JhmxXXubqKWX2CEAMNXt\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIxM0pobXhYWHVicUtXWDJDRUFNTlh0Iiwib3JnYW5pemF0aW9uSWQiOjczMTk2LCJ1c2VySWQiOjg1MDg2LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiJhZTgzNmEwZS05YmU4LTQwZmEtOWFlNi1iYmEwZDRkNDM0MjIifX0.6AEuwe2V_ZhEKSOCzkpQhuZwTFcGPV1vcGBSNwBEKlk\";");
prop.setProperty("sasl.mechanism", "PLAIN");


//set producer property

prop.setProperty("key.serializer", StringSerializer.class.getName());
prop.setProperty("value.serializer", StringSerializer.class.getName());
//create producer
KafkaProducer<String, String> kafkaProducer=new KafkaProducer<String, String>(prop);

//create producer record
ProducerRecord<String, String> producerRecord=new ProducerRecord<String, String>("test_topic", "test_value");

//send data
kafkaProducer.send(producerRecord);
//flush and close the producer
kafkaProducer.flush();

kafkaProducer.close();
	}

}
