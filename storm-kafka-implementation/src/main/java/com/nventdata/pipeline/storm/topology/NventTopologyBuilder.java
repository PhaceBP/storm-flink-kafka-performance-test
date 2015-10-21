package com.nventdata.pipeline.storm.topology;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.nventdata.pipeline.configuration.ApplicationConfig;
import com.nventdata.pipeline.storm.bolt.KafkaMessageForwarderBolt;

import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

@Component
public class NventTopologyBuilder {
	
	@Autowired
	private KafkaMessageForwarderBolt bolt;
	
	@Autowired
	private ApplicationConfig config;
	
	private BrokerHosts brokerHosts;
	
	@PostConstruct
	public void init(){
		 brokerHosts = new ZkHosts(config.getZookeperUrl());
	}
	
    public StormTopology buildTopology() {
        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, config.getSourceTopicName(), "", "storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("datas", new KafkaSpout(kafkaConfig), 10);
        builder.setBolt("forward", bolt).shuffleGrouping("datas");
        return builder.createTopology();
    }

}
