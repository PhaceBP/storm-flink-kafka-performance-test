package com.nventdata.pipeline.storm.bolt;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.IOUtils;

import com.nventdata.pipeline.avro.model.NventMessage;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaMessageProducer {

	private static Producer<String, byte[]> kafkaProducer;
	private static final SpecificDatumWriter<NventMessage> avroMessageWriter = new SpecificDatumWriter<NventMessage>(
			NventMessage.SCHEMA$);
	private static final EncoderFactory avroEncoderFactory = EncoderFactory.get();
	public static final String KAFKA_BROKER_PROPERTIES = "kafka.broker.properties";


	@SuppressWarnings("rawtypes")
	public static void initProducer(Map stormConf) {
		Properties prop = transformPropertiesMap((Map) stormConf.get(KAFKA_BROKER_PROPERTIES));
		kafkaProducer = new Producer<String, byte[]>(new ProducerConfig(prop));
	}
	
	public static void publish(NventMessage message, String topicName) {
		try {
			ByteArrayOutputStream stream = new ByteArrayOutputStream();
			BinaryEncoder binaryEncoder = avroEncoderFactory.binaryEncoder(stream, null);
			avroMessageWriter.write(message, binaryEncoder);
			binaryEncoder.flush();
			IOUtils.closeQuietly(stream);
			kafkaProducer.send(new KeyedMessage<String, byte[]>(topicName, stream.toByteArray()));
		} catch (IOException e) {
			throw new RuntimeException("Avro serialization failure", e);
		}
	}

	@SuppressWarnings("rawtypes")
	private static Properties transformPropertiesMap(Map stormConfig) {
		Properties prop = new Properties();
		for (Object o : stormConfig.keySet()) {
			prop.put(o, stormConfig.get(o));
		}
		return prop;
	}
}
