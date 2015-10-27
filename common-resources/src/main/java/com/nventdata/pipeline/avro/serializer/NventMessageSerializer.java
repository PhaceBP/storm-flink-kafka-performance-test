package com.nventdata.pipeline.avro.serializer;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.nventdata.pipeline.avro.model.NventMessage;

public class NventMessageSerializer extends Serializer<NventMessage> implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3553379134129010814L;
	
	private static final Logger LOG = LoggerFactory.getLogger(NventMessageSerializer.class);

	public void write(Kryo kryo, Output output, NventMessage object) {

		if (LOG.isDebugEnabled()) {
			LOG.debug("Message received in Storm serializer ==> {}", object);
		}
		
		byte[] outBytes = AvroMessageSerializationUtils.write(object);
		output.writeInt(outBytes.length, true);
		output.write(outBytes);
	}

	public NventMessage read(Kryo kryo, Input input, Class<NventMessage> type) {

		byte[] value = input.getBuffer();
		NventMessage message = AvroMessageSerializationUtils.read(value);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Message received in Storm deserializer ==> {}", message);
		}

		return message;
	}
}