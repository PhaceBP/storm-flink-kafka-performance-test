package com.nventdata.pipeline.storm.bolt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.nventdata.pipeline.avro.model.NventMessage;
import com.nventdata.pipeline.kafka.producer.KafkaAvroMessageProducer;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

@Component
public class KafkaMessageForwarderBolt extends BaseBasicBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Autowired
	private transient KafkaAvroMessageProducer producer;

	public static final Logger LOG = LoggerFactory.getLogger(KafkaMessageForwarderBolt.class);

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {

		LOG.info("Message received: " + tuple.toString());

		Integer random = tuple.getIntegerByField("random");

		String topicName = null;

		switch (random) {

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

		NventMessage forwardedMessage = NventMessage.newBuilder().setId(tuple.getIntegerByField("id"))
				.setData(tuple.getStringByField("data")).setRandom(random).build();

		producer.publish(forwardedMessage, topicName);

	}

}
