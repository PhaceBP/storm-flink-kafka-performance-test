package com.nventdata.pipeline.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ApplicationConfig {
	
	@Value("${zookeeper.host}")
	private String zookeeperHost;
	
	@Value("${zookeeper.port}")
	private Integer zookeeperPort;
	
	@Value("${kafka.host}")
	private String kafkaHost;
	
	@Value("${kafka.port}")
	private Integer kafkaPort;
	
	@Value("${source.topic.name}")
	private String sourceTopicName;

	public String getZookeeperHost() {
		return zookeeperHost;
	}

	public void setZookeeperHost(String zookeeperHost) {
		this.zookeeperHost = zookeeperHost;
	}

	public Integer getZookeeperPort() {
		return zookeeperPort;
	}

	public void setZookeeperPort(Integer zookeeperPort) {
		this.zookeeperPort = zookeeperPort;
	}

	public String getKafkaHost() {
		return kafkaHost;
	}

	public void setKafkaHost(String kafkaHost) {
		this.kafkaHost = kafkaHost;
	}

	public Integer getKafkaPort() {
		return kafkaPort;
	}

	public void setKafkaPort(Integer kafkaPort) {
		this.kafkaPort = kafkaPort;
	}
	
	public String getZookeperUrl(){
		return zookeeperHost + ":" + zookeeperPort.intValue();
	}
	
	public String getKafkaUrl(){
		return kafkaHost + ":" + kafkaPort.intValue();
	}

	public String getSourceTopicName() {
		return sourceTopicName;
	}

	public void setSourceTopicName(String sourceTopicName) {
		this.sourceTopicName = sourceTopicName;
	}
}
