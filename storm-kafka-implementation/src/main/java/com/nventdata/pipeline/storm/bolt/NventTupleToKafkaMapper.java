package com.nventdata.pipeline.storm.bolt;

import com.nventdata.pipeline.avro.model.NventMessage;
import com.nventdata.pipeline.storm.serializer.NventMessageAvroSchema;

import backtype.storm.tuple.Tuple;
import storm.kafka.bolt.mapper.TupleToKafkaMapper;

public class NventTupleToKafkaMapper implements TupleToKafkaMapper<String, NventMessage> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public String getKeyFromTuple(Tuple tuple) {
		return NventMessageAvroSchema.FIELD_NAME;
	}

	@Override
	public NventMessage getMessageFromTuple(Tuple tuple) {
		return (NventMessage) tuple.getValueByField(NventMessageAvroSchema.FIELD_NAME);
	}

}
