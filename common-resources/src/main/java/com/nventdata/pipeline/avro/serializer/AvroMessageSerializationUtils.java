package com.nventdata.pipeline.avro.serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.IOUtils;
import com.nventdata.pipeline.avro.model.NventMessage;

public final class AvroMessageSerializationUtils {

	public static final byte[] write(NventMessage obj) {
		DatumWriter<NventMessage> writer = new SpecificDatumWriter<>(NventMessage.SCHEMA$);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
		try {
			writer.write(obj, encoder);
			encoder.flush();
		} catch (IOException e) {
			System.err.println(e);
		}
		IOUtils.closeQuietly(out);
		byte[] outBytes = out.toByteArray();
		return outBytes;
	}

	public static final NventMessage read(String message) {
		return read(message.getBytes());
	}

	public static final NventMessage read(byte[] input) {

		SpecificDatumReader<NventMessage> reader = new SpecificDatumReader<>(NventMessage.SCHEMA$);
		NventMessage record = null;
		try {
			record = reader.read(null, DecoderFactory.get().binaryDecoder(input, null));
		} catch (IOException e) {
			System.err.println(e);
		}
		return record;
	}

}
