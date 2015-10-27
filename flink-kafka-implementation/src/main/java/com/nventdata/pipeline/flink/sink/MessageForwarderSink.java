package com.nventdata.pipeline.flink.sink;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaSink;
import org.apache.flink.streaming.util.serialization.JavaDefaultStringSchema;

import com.nventdata.pipeline.avro.model.NventMessage;
import com.nventdata.pipeline.flink.mapper.TopsicSelectorEvaluationResultHolder;
import com.nventdata.pipline.flink.environment.ExecutionEnvironmentFactory;

/*
 * This class is responsible for send a message to the Kafka broker
 */
public class MessageForwarderSink implements SinkFunction<TopsicSelectorEvaluationResultHolder> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6213994719614977911L;

	@Override
	public void invoke(final TopsicSelectorEvaluationResultHolder value) throws Exception {

		// create execution environment
		StreamExecutionEnvironment env = ExecutionEnvironmentFactory.createStreamExecutionEvironment();
		env.getConfig().enableForceAvro();

		// parse user parameters
		ParameterTool parameterTool = ExecutionEnvironmentFactory.createParameterTool(value.getSelectedTopicName());

		List<String> messageList = new ArrayList<>();
		messageList.add(NventMessage.newBuilder().setId(value.getId()).setData(value.getData())
				.setRandom(value.getRandom()).build().toString());

		DataStream<String> messageStream = env.fromCollection(messageList);

		messageStream.addSink(new KafkaSink<String>(parameterTool.getRequired("bootstrap.servers"), parameterTool.getRequired("topic"), new JavaDefaultStringSchema()));

		env.execute();
	}
}
