package com.nventdata.pipeline.storm.serializer;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.io.Input;
import com.nventdata.pipeline.avro.model.NventMessage;
import com.nventdata.pipeline.avro.serializer.NventMessageSerializer;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;

public class NventMessageAvroSchema implements Scheme {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(NventMessageAvroSchema.class);
	public static final String FIELD_NAME = "message";

	// deserializing the message relieved by the Spout
	public List<Object> deserialize(byte[] pojoBytes) {
		
		NventMessageSerializer serializer = new NventMessageSerializer(); // Kryo
																			// Serializer
		NventMessage message = serializer.read(null, new Input(pojoBytes), NventMessage.class);
		
		LOG.debug("Meesage received in NventMessageAvroSchema: {}",message);
		
		List<Object> values = new ArrayList<>();
		values.add(0, message);
		return values;
	}

	// defining the output fields of the Spout
	public Fields getOutputFields() {
		return new Fields(new String[] { FIELD_NAME});
	}
}