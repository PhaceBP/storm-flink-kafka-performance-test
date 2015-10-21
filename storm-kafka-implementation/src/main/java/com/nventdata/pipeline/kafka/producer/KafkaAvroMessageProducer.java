package com.nventdata.pipeline.kafka.producer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import javax.annotation.PostConstruct;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.nventdata.pipeline.avro.model.NventMessage;
import com.nventdata.pipeline.configuration.ApplicationConfig;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

@Component
public class KafkaAvroMessageProducer implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private Producer<String, byte[]> kafkaProducer;
	private final SpecificDatumWriter<NventMessage> avroMessageWriter = new SpecificDatumWriter<NventMessage>(
			NventMessage.SCHEMA$);
	private final EncoderFactory avroEncoderFactory = EncoderFactory.get();

	@Autowired
	private ApplicationConfig config;

	@PostConstruct
	public void init() {
		Properties props = new Properties();
		props.put("zk.connect", config.getZookeperUrl());
		props.put("metadata.broker.list", config.getKafkaUrl());
		props.put("serializer.class", "kafka.serializer.DefaultEncoder");
		props.put("client.id", "nvent");
		kafkaProducer = new Producer<String, byte[]>(new ProducerConfig(props));
	}

	public void publish(NventMessage message,String topicName) {
		try {
			ByteArrayOutputStream stream = new ByteArrayOutputStream();
			BinaryEncoder binaryEncoder = avroEncoderFactory.binaryEncoder(stream, null);
			avroMessageWriter.write(message, binaryEncoder);
			binaryEncoder.flush();
			IOUtils.closeQuietly(stream);

			kafkaProducer.send(new KeyedMessage<String, byte[]>(topicName, stream.toByteArray()));
		} catch (IOException e) {
			throw new RuntimeException("Avro serialization failure", e);
		} finally {
			kafkaProducer.close();
		}
	}
}