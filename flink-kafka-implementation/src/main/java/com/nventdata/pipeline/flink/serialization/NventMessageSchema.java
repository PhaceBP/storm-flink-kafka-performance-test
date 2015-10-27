package com.nventdata.pipeline.flink.serialization;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

import com.nventdata.pipeline.avro.model.NventMessage;
import com.nventdata.pipeline.avro.serializer.AvroMessageSerializationUtils;

public class NventMessageSchema implements DeserializationSchema<NventMessage>,
SerializationSchema<NventMessage, byte[]>{

	/**
	 * 
	 */
	private static final long serialVersionUID = -6769252986074105761L;

	@Override
	public TypeInformation<NventMessage> getProducedType() {
		return TypeExtractor.getForClass(NventMessage.class);
	}

	@Override
	public byte[] serialize(NventMessage element) {
		return AvroMessageSerializationUtils.write(element);
	}

	@Override
	public NventMessage deserialize(byte[] message) {
		return AvroMessageSerializationUtils.read(message);
	}

	@Override
	public boolean isEndOfStream(NventMessage nextElement) {
		return false;
	}

}
