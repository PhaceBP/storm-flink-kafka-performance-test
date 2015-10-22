package com.nventdata.pipeline.storm.bolt;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nventdata.pipeline.avro.model.NventMessage;
import com.nventdata.pipeline.kafka.producer.KafkaAvroMessageProducer;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class KafkaMessageForwarderBolt extends BaseBasicBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static final Logger LOG = LoggerFactory.getLogger(KafkaMessageForwarderBolt.class);

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {

		LOG.info("Message received: " + tuple.toString());

		String avroMessage = (String) tuple.getString(0);
		NventMessage deserializedMessage = null;
		try {
			
			DatumReader<NventMessage> datumReader = new SpecificDatumReader<NventMessage>(NventMessage.SCHEMA$);
			ByteArrayInputStream stream = new ByteArrayInputStream(avroMessage.getBytes());
			BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(stream, null);
			deserializedMessage = datumReader.read(null, binaryDecoder);
			IOUtils.closeQuietly(stream);
		} catch (IOException ex) {
			LOG.error("Error occured while deserializing message : " + ex.getMessage());
			return;
		}

		String topicName = null;

		switch (deserializedMessage.getRandom()) {

		case 1:
			topicName = "random1";
			break;
		case 2:
			topicName = "random2";
			break;
		case 3:
			topicName = "random3";
			break;
		default:
			throw new IllegalArgumentException("Random value from the message must beetween 1 and 3");
		}

		KafkaAvroMessageProducer.publish(deserializedMessage, topicName);

	}

}
