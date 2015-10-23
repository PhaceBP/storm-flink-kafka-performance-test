package com.nventdata.pipeline.storm.bolt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nventdata.pipeline.avro.model.NventMessage;
import com.nventdata.pipeline.storm.serializer.NventMessageAvroSchema;

import backtype.storm.tuple.Tuple;
import storm.kafka.bolt.selector.KafkaTopicSelector;

public class NventKafkaTopicSelector implements KafkaTopicSelector {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public static final Logger LOG = LoggerFactory.getLogger(NventKafkaTopicSelector.class);

	@Override
	public String getTopic(Tuple tuple) {

		LOG.info("The following message received: {}", tuple.toString());

		NventMessage message = (NventMessage) tuple.getValueByField(NventMessageAvroSchema.FIELD_NAME);

		String topicName = null;

		switch (message.getRandom()) {

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

		return topicName;
	}

}
