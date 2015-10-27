package com.nventdata.pipeline;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import com.nventdata.pipeline.configuration.ApplicationConfig;
import com.nventdata.pipeline.flink.messagerouter.FlinkKafkaTopicSelector;
import com.nventdata.pipline.flink.environment.ExecutionEnvironmentFactory;

@SpringBootApplication
public class Application {

	public static void main(String[] args) throws Exception {
		ConfigurableApplicationContext ctx = SpringApplication.run(Application.class, args);
		ApplicationConfig config = ctx.getBean(ApplicationConfig.class);
		ExecutionEnvironmentFactory.initConfig(config);
		FlinkKafkaTopicSelector.consumeMessageAndForwardToKafka(config.getSourceTopicName());
	}

}
