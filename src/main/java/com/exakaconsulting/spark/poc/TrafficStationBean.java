package com.exakaconsulting.spark.poc;

import java.io.Serializable;
import java.util.List;

public class TrafficStationBean implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 8820607214452314228L;
	
	private Long id;
	
	private String reseau;
	
	private String station;
	
	private Long traffic;
	
	private List<String> listCorrespondance;
	
	private String ville;
	
	private Long arrondissement;
	
	

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getReseau() {
		return reseau;
	}

	public void setReseau(String reseau) {
		this.reseau = reseau;
	}

	public String getStation() {
		return station;
	}

	public void setStation(String station) {
		this.station = station;
	}

	public Long getTraffic() {
		return traffic;
	}

	public void setTraffic(Long traffic) {
		this.traffic = traffic;
	}


	public List<String> getListCorrespondance() {
		return listCorrespondance;
	}

	
	@Override
	public String toString() {
		return "TrafficStationBean [id=" + id + ", reseau=" + reseau + ", station=" + station + ", traffic=" + traffic
				+ ", listCorrespondance=" + listCorrespondance + ", ville=" + ville + ", arrondissement="
				+ arrondissement + "]";
	}

	public void setListCorrespondance(List<String> listCorrespondance) {
		this.listCorrespondance = listCorrespondance;
	}

	public String getVille() {
		return ville;
	}

	public void setVille(String ville) {
		this.ville = ville;
	}

	public Long getArrondissement() {
		return arrondissement;
	}

	public void setArrondissement(Long arrondissement) {
		this.arrondissement = arrondissement;
	}
	
	
}
