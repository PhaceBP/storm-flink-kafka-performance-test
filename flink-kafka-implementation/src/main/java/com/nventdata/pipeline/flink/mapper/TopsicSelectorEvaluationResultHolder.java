package com.nventdata.pipeline.flink.mapper;

import java.io.Serializable;

public class TopsicSelectorEvaluationResultHolder implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2377282296272763672L;

	private Integer id;
	
	private Integer random;
	
	private CharSequence data;
	
	private String selectedTopicName;

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public Integer getRandom() {
		return random;
	}

	public void setRandom(Integer random) {
		this.random = random;
	}

	public CharSequence getData() {
		return data;
	}

	public void setData(CharSequence data) {
		this.data = data;
	}

	public String getSelectedTopicName() {
		return selectedTopicName;
	}

	public void setSelectedTopicName(String selectedTopicName) {
		this.selectedTopicName = selectedTopicName;
	}

	@Override
	public String toString() {
		return "TopsicSelectorEvaluationResultHolder [id=" + id + ", random=" + random + ", data=" + data
				+ ", selectedTopicName=" + selectedTopicName + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((data == null) ? 0 : data.hashCode());
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((random == null) ? 0 : random.hashCode());
		result = prime * result + ((selectedTopicName == null) ? 0 : selectedTopicName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TopsicSelectorEvaluationResultHolder other = (TopsicSelectorEvaluationResultHolder) obj;
		if (data == null) {
			if (other.data != null)
				return false;
		} else if (!data.equals(other.data))
			return false;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (random == null) {
			if (other.random != null)
				return false;
		} else if (!random.equals(other.random))
			return false;
		if (selectedTopicName == null) {
			if (other.selectedTopicName != null)
				return false;
		} else if (!selectedTopicName.equals(other.selectedTopicName))
			return false;
		return true;
	}
	
	
	
}
