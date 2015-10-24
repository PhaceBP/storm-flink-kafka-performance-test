package com.nventdata.pipeline.storm.bolt;

import java.util.Map;

import com.nventdata.pipeline.avro.model.NventMessage;
import com.nventdata.pipeline.storm.serializer.NventMessageAvroSchema;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import storm.kafka.bolt.selector.KafkaTopicSelector;

public class KafkaProducerBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private KafkaTopicSelector topicSelector;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		topicSelector = new NventKafkaTopicSelector();
		KafkaMessageProducer.initProducer(stormConf);
	}

	@Override
	public void execute(Tuple input) {
		String topicName = topicSelector.getTopic(input);
		NventMessage message = (NventMessage) input.getValueByField(NventMessageAvroSchema.FIELD_NAME);
		KafkaMessageProducer.publish(message, topicName);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}
}
