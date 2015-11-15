package com.nventdata.pipeline;

import com.nventdata.pipeline.configuration.CommonConfiguration;
import com.nventdata.pipeline.configuration.ConfigurationLoader;
import com.nventdata.pipeline.flink.messagerouter.FlinkKafkaTopicSelector;

public class Application {

	public static void main(String[] args) throws Exception {

		CommonConfiguration conf = ConfigurationLoader.getInstance();
		FlinkKafkaTopicSelector.consumeMessageAndForwardToKafka(conf.getSourceTopicName());
	}

}
