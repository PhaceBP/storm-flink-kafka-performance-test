package com.nventdata.pipeline;

import java.util.Map;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import com.nventdata.pipeline.avro.model.NventMessage;
import com.nventdata.pipeline.avro.serializer.NventMessageSerializer;
import com.nventdata.pipeline.storm.topology.NventTopologyBuilder;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import storm.kafka.bolt.KafkaBolt;

@SpringBootApplication
public class Application {

	public static void main(String[] args) throws Exception {
		ConfigurableApplicationContext ctx = SpringApplication.run(Application.class, args);
		Config config = new Config();
		config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);

		Map<String, String> kafkaProperties = new java.util.HashMap<>();
		kafkaProperties.put("zk.connect", "localhost:2181");
		kafkaProperties.put("metadata.broker.list", "localhost:9092");
		kafkaProperties.put("serializer.class", "kafka.serializer.DefaultEncoder");
		kafkaProperties.put("client.id", "nvent");
		config.registerSerialization(NventMessage.class, NventMessageSerializer.class);
		config.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, kafkaProperties);
		config.setNumWorkers(2);
		config.setMaxTaskParallelism(2);

		LocalCluster cluster = new LocalCluster();
		NventTopologyBuilder topologyBuilder = ctx.getBean(NventTopologyBuilder.class);
		cluster.submitTopology("kafka", config, topologyBuilder.buildTopology());

	}
}
