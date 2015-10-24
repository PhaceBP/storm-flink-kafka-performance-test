package com.nventdata.pipeline.storm.bolt;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.IOUtils;

import com.nventdata.pipeline.avro.model.NventMessage;
import com.nventdata.pipeline.storm.serializer.NventMessageAvroSchema;

import backtype.storm.tuple.Tuple;
import storm.kafka.bolt.mapper.TupleToKafkaMapper;

public class NventTupleToKafkaMapper implements TupleToKafkaMapper<String, byte[]> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public String getKeyFromTuple(Tuple tuple) {
		return NventMessageAvroSchema.FIELD_NAME;
	}

	@Override
	public byte[] getMessageFromTuple(Tuple tuple) {
		NventMessage message = (NventMessage) tuple.getValueByField(NventMessageAvroSchema.FIELD_NAME);
		
		DatumWriter<NventMessage> writer = new SpecificDatumWriter<>(NventMessage.SCHEMA$);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
		try {
			writer.write(message, encoder);
			encoder.flush();
		} catch (IOException e) {
			System.err.println(e.getMessage());
		}
		IOUtils.closeQuietly(out);
		return out.toByteArray();
	}

}
