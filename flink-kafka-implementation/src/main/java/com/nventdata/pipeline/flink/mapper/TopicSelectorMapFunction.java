package com.nventdata.pipeline.flink.mapper;

import org.apache.flink.api.common.functions.MapFunction;

import com.nventdata.pipeline.avro.model.NventMessage;

public class TopicSelectorMapFunction
		implements MapFunction<NventMessage, TopsicSelectorEvaluationResultHolder> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6054109411881125341L;

	@Override
	public TopsicSelectorEvaluationResultHolder map(NventMessage value) throws Exception {

		String topicName = null;

		switch (value.getRandom()) {

		case 1:
			topicName = "random1";
			break;
		case 2:
			topicName = "random2";
			break;
		case 3:
			topicName = "random3";
			break;
		default:
			throw new IllegalArgumentException("Random value from the message must beetween 1 and 3");
		}

		TopsicSelectorEvaluationResultHolder result = new TopsicSelectorEvaluationResultHolder();
		result.setData(value.getData());
		result.setId(value.getId());
		result.setRandom(value.getRandom());
		result.setSelectedTopicName(topicName);

		return result;
	}

}
