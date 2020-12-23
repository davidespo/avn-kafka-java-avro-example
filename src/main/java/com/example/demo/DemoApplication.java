package com.example.demo;

import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import java.io.File;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;


@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}

	@Bean
	public static CommandLineRunner avroTest(Environment env) {

		return (args) -> {
			Properties props = new Properties();
			props.put("bootstrap.servers", env.getProperty("bootstrap.servers"));
			props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
			props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
			props.put("schema.registry.url", env.getProperty("schema.registry.url"));
			props.put("basic.auth.credentials.source", "USER_INFO");
			props.put("basic.auth.user.info", env.getProperty("basic.auth.user.info"));
			props.put("security.protocol", "SSL"); 
			props.put("ssl.endpoint.identification.algorithm", "");
			props.put("ssl.truststore.location", "client.truststore.jks");
			props.put("ssl.truststore.password", env.getProperty("ssl.truststore.password", "secret"));
			props.put("ssl.keystore.type", "PKCS12");
			props.put("ssl.keystore.location", "client.keystore.p12");
			props.put("ssl.keystore.password", env.getProperty("ssl.keystore.password", "secret"));
			props.put("ssl.key.password", env.getProperty("ssl.key.password", "secret"));

			KafkaProducer producer = new KafkaProducer(props);

			Schema keySchema = new Schema.Parser().parse(new File("key.avsc"));

			Schema valueSchema = new Schema.Parser().parse(new File("value.avsc"));

			GenericRecord keyRecord = new GenericData.Record(keySchema);

			GenericRecord valueRecord = new GenericData.Record(valueSchema);

			keyRecord.put("pk", "TESTING");
			valueRecord.put("ev", "xt");
			valueRecord.put("x", "BITSTAMP");
			valueRecord.put("t", "ETH");
			valueRecord.put("tp", "USD");
			valueRecord.put("rpk","xt.BITSTAMP.XRP.USD");

			ProducerRecord<GenericRecord, GenericRecord> record = new ProducerRecord<>(
					"qfy-avro",
					keyRecord,
					valueRecord
			);

			try {
				long startTime = System.currentTimeMillis();
				for (int i=0;i<3;i++) {
					Future future = producer.send(record);
					Object val = future.get(30, TimeUnit.SECONDS);
					System.out.println("val = " + val);
					System.out.println("future.isDone() = " + future.isDone());
				}
				producer.flush();
				producer.close();
				long duration = System.currentTimeMillis() - startTime;
				System.out.println("DONE! Finished in " + duration + "ms");

			} catch(SerializationException e) {
				System.out.println("FAILED!");
				System.out.println(e.toString());
				System.out.println(record.value());
			}
		};
	}
}
