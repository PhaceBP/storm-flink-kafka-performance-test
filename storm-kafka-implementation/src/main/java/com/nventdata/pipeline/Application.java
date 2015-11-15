package com.nventdata.pipeline;

import java.util.Map;

import com.nventdata.pipeline.avro.model.NventMessage;
import com.nventdata.pipeline.avro.serializer.NventMessageSerializer;
import com.nventdata.pipeline.configuration.CommonConfiguration;
import com.nventdata.pipeline.configuration.ConfigurationLoader;
import com.nventdata.pipeline.storm.topology.NventTopologyBuilder;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import storm.kafka.bolt.KafkaBolt;

public class Application {

	public static void main(String[] args) throws Exception {
	
		CommonConfiguration commonConfig = ConfigurationLoader.getInstance();
		Config config = new Config();
		config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);

		Map<String, String> kafkaProperties = new java.util.HashMap<>();
		kafkaProperties.put("zk.connect", commonConfig.getZookeperUrl());
		kafkaProperties.put("metadata.broker.list", commonConfig.getKafkaUrl());
		kafkaProperties.put("serializer.class", "kafka.serializer.DefaultEncoder");
		kafkaProperties.put("client.id", "nvent");
		config.registerSerialization(NventMessage.class, NventMessageSerializer.class);
		config.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, kafkaProperties);
		config.setNumWorkers(2);
		config.setMaxTaskParallelism(2);

		LocalCluster cluster = new LocalCluster();
		NventTopologyBuilder topologyBuilder = new NventTopologyBuilder();
		cluster.submitTopology("kafka", config, topologyBuilder.buildTopology());

	}
}
