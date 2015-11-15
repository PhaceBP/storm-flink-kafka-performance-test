package com.nventdata.pipeline.configuration;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigurationLoader {

	private static CommonConfiguration instance;

	private ConfigurationLoader() {

	}

	private static final CommonConfiguration createConfigurationInstance() {

		InputStream in = ConfigurationLoader.class.getResourceAsStream("/config/application.properties");
		
		if(in == null){
			throw new IllegalStateException("Missing configuration properties in classpath!");
		}
		Properties props = new Properties();
		CommonConfiguration commonConfig = null;
		try {
			props.load(in);
			commonConfig = new CommonConfiguration(props);
		} catch (

		IOException e1)

		{
			e1.printStackTrace();
		}
		return commonConfig;
	}

	public static synchronized CommonConfiguration getInstance() {
		if (instance == null) {
			instance = createConfigurationInstance();
		}
		return instance;
	}

}
