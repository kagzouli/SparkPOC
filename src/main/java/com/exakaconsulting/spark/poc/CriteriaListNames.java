package com.exakaconsulting.spark.poc;

import java.io.Serializable;
import java.util.List;

public class CriteriaListNames implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 7673212933748100822L;
	
	
	private List<String> request;


	public List<String> getRequest() {
		return request;
	}


	public void setRequest(List<String> request) {
		this.request = request;
	}
	
	
	
	

}
