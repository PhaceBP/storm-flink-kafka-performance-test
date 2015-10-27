package com.nventdata.pipeline.flink.messagerouter;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;

import com.nventdata.pipeline.avro.model.NventMessage;
import com.nventdata.pipeline.flink.mapper.TopicSelectorMapFunction;
import com.nventdata.pipeline.flink.serialization.NventMessageSchema;
import com.nventdata.pipeline.flink.sink.MessageForwarderSink;
import com.nventdata.pipline.flink.environment.ExecutionEnvironmentFactory;


public class FlinkKafkaTopicSelector {

	public static JobExecutionResult consumeMessageAndForwardToKafka(String topicN) throws Exception {

		StreamExecutionEnvironment env = ExecutionEnvironmentFactory.createStreamExecutionEvironment();
		ParameterTool parameterTool = ExecutionEnvironmentFactory.createParameterTool(topicN);

		DataStream<NventMessage> messageStream = env.addSource(new FlinkKafkaConsumer082<>(
				parameterTool.getRequired("topic"), new NventMessageSchema(), parameterTool.getProperties()));

		messageStream.rebalance().map(new TopicSelectorMapFunction())
		.addSink(new MessageForwarderSink());
		
		JobExecutionResult result = env.execute();
		
		return result;
	}
}
