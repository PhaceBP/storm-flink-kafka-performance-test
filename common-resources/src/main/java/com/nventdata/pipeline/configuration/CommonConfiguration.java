package com.nventdata.pipeline.configuration;

import java.util.Properties;

public class CommonConfiguration {

	private Properties props;

	public CommonConfiguration(Properties props) {
		this.props = props;
	}

	public String getZookeeperHost() {
		return props.getProperty("zookeeper.host");
	}

	public Integer getZookeeperPort() {
		return Integer.valueOf(props.getProperty("zookeeper.port"));
	}

	public String getKafkaHost() {
		return props.getProperty("kafka.host");
	}

	public Integer getKafkaPort() {
		return Integer.valueOf(props.getProperty("kafka.port"));
	}

	public String getZookeperUrl() {
		return getZookeeperHost() + ":" + getZookeeperPort();
	}

	public String getKafkaUrl() {
		return getKafkaHost() + ":" + getKafkaPort();
	}

	public String getSourceTopicName() {
		return props.getProperty("source.topic.name");
	}
}
