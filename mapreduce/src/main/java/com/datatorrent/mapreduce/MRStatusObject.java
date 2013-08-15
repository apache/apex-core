package com.datatorrent.mapreduce;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.sf.json.JSON;
import net.sf.json.JSONObject;

public class MRStatusObject {
	private String uri;
	private String jobId;
	private String apiVersion;
	private String hadoopVersion;
	private String appId;
	private int rmPort;
	private int historyServerPort;
	private JSONObject jsonObject;
	private Map<String, JSONObject> mapJsonObject;
	private Map<String, JSONObject> reduceJsonObject;
	
	public MRStatusObject(){
		mapJsonObject = new ConcurrentHashMap<String, JSONObject>();
		reduceJsonObject = new ConcurrentHashMap<String, JSONObject>();
	}
	
	
	public Map<String, JSONObject> getMapJsonObject() {
		return mapJsonObject;
	}
	public void setMapJsonObject(Map<String, JSONObject> mapJsonObject) {
		this.mapJsonObject = mapJsonObject;
	}
	public Map<String, JSONObject> getReduceJsonObject() {
		return reduceJsonObject;
	}
	public void setReduceJsonObject(Map<String, JSONObject> reduceJsonObject) {
		this.reduceJsonObject = reduceJsonObject;
	}
	public String getUri() {
		return uri;
	}
	public void setUri(String uri) {
		this.uri = uri;
	}
	public String getJobId() {
		return jobId;
	}
	public void setJobId(String jobId) {
		this.jobId = jobId;
	}
	public String getApiVersion() {
		return apiVersion;
	}
	public void setApiVersion(String apiVersion) {
		this.apiVersion = apiVersion;
	}
	public String getHadoopVersion() {
		return hadoopVersion;
	}
	public void setHadoopVersion(String hadoopVersion) {
		this.hadoopVersion = hadoopVersion;
	}
	public String getAppId() {
		return appId;
	}
	public void setAppId(String appId) {
		this.appId = appId;
	}
	public int getRmPort() {
		return rmPort;
	}
	public void setRmPort(int rmPort) {
		this.rmPort = rmPort;
	}
	public int getHistoryServerPort() {
		return historyServerPort;
	}
	public void setHistoryServerPort(int historyServerPort) {
		this.historyServerPort = historyServerPort;
	}
	public JSONObject getJsonObject() {
		return jsonObject;
	}
	public void setJsonObject(JSONObject jsonObject) {
		this.jsonObject = jsonObject;
	}
	
	

}
