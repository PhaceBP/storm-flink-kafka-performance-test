package com.nventdata.pipeline.storm.topology;

import com.nventdata.pipeline.configuration.CommonConfiguration;
import com.nventdata.pipeline.configuration.ConfigurationLoader;
import com.nventdata.pipeline.storm.bolt.KafkaProducerBolt;
import com.nventdata.pipeline.storm.serializer.NventMessageAvroSchema;

import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;


public class NventTopologyBuilder {

	private BrokerHosts brokerHosts;
	
	private CommonConfiguration config;
	
	public NventTopologyBuilder(){
		config = ConfigurationLoader.getInstance();
		brokerHosts = new ZkHosts(config.getZookeperUrl());
	}

	public StormTopology buildTopology() {
		SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, config.getSourceTopicName(), "", "storm");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new NventMessageAvroSchema());
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("dataSource", new KafkaSpout(kafkaConfig), 10);
		builder.setBolt("forwarder", new KafkaProducerBolt()).shuffleGrouping("dataSource");
		return builder.createTopology();
	}

}
