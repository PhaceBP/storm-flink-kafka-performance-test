package com.nventdata.pipeline.flink.environment;

import java.util.Map;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.nventdata.pipeline.avro.model.NventMessage;
import com.nventdata.pipeline.avro.serializer.NventMessageSerializer;
import com.nventdata.pipeline.configuration.CommonConfiguration;
import com.nventdata.pipeline.configuration.ConfigurationLoader;


public class ExecutionEnvironmentFactory {

	
	public static StreamExecutionEnvironment createStreamExecutionEvironment(){
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().registerTypeWithKryoSerializer(NventMessage.class, new NventMessageSerializer());

		return env;
	}
	
	public static ParameterTool createParameterTool(String topicName){

		CommonConfiguration configuration = ConfigurationLoader.getInstance();
		Map<String, String> kafkaProperties = new java.util.HashMap<>();
		kafkaProperties.put("zookeeper.connect", configuration.getZookeperUrl());
		kafkaProperties.put("bootstrap.servers", configuration.getKafkaUrl());
		kafkaProperties.put("topic", topicName);
		kafkaProperties.put("group.id", "nvent");

		ParameterTool parameterTool = ParameterTool.fromMap(kafkaProperties);
		
		return parameterTool;

	}
}
