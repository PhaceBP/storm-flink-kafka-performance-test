package com.nventdata.pipeline.kafka.producer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.IOUtils;

import com.nventdata.pipeline.avro.model.NventMessage;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaAvroMessageProducer implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static Producer<String, byte[]> kafkaProducer;
	private static final SpecificDatumWriter<NventMessage> avroMessageWriter = new SpecificDatumWriter<NventMessage>(
			NventMessage.SCHEMA$);
	private static final EncoderFactory avroEncoderFactory = EncoderFactory.get();

	
	static {
		Properties props = new Properties();
		props.put("zk.connect", "localhost:2181");
		props.put("metadata.broker.list", "localhost:9092");
		props.put("serializer.class", "kafka.serializer.DefaultEncoder");
		props.put("client.id", "nvent");
		kafkaProducer = new Producer<String, byte[]>(new ProducerConfig(props));
	}

	public static void publish(NventMessage message,String topicName) {
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