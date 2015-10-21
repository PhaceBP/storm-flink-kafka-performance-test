package com.nventdata.pipeline;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import com.nventdata.pipeline.storm.topology.NventTopologyBuilder;

import backtype.storm.Config;
import backtype.storm.LocalCluster;

@SpringBootApplication
public class Application {

	public static void main(String[] args) throws Exception {
		ConfigurableApplicationContext ctx = SpringApplication.run(Application.class, args);
		Config config = new Config();
        config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);
        config.setNumWorkers(2);
        config.setMaxTaskParallelism(2);
		LocalCluster cluster = new LocalCluster();
		NventTopologyBuilder topologyBuilder = ctx.getBean(NventTopologyBuilder.class);
        cluster.submitTopology("kafka", config, topologyBuilder.buildTopology());
		
	}
}
