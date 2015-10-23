package com.nventdata.pipeline.storm.serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.nventdata.pipeline.avro.model.NventMessage;

public class NventMessageSerializer extends Serializer<NventMessage> {

	private static final Logger LOG = LoggerFactory.getLogger(NventMessageSerializer.class);
	private Schema SCHEMA = NventMessage.SCHEMA$;

	public void write(Kryo kryo, Output output, NventMessage object) {
		DatumWriter<NventMessage> writer = new SpecificDatumWriter<>(SCHEMA);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
		try {
			writer.write(object, encoder);
			encoder.flush();
		} catch (IOException e) {
			LOG.error(e.toString(), e);
		}
		IOUtils.closeQuietly(out);
		byte[] outBytes = out.toByteArray();
		output.writeInt(outBytes.length, true);
		output.write(outBytes);
	}

	public NventMessage read(Kryo kryo, Input input, Class<NventMessage> type) {
		byte[] value = input.getBuffer();
		SpecificDatumReader<NventMessage> reader = new SpecificDatumReader<>(SCHEMA);
		NventMessage record = null;
		try {
			record = reader.read(null, DecoderFactory.get().binaryDecoder(value, null));
		} catch (IOException e) {
			LOG.error(e.toString(), e);
		}
		return record;
	}
}