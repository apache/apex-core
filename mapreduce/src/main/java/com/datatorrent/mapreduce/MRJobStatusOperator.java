package com.datatorrent.mapreduce;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.ShipContainingJars;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.IdleTimeHandler;
import com.datatorrent.api.Operator;

@ShipContainingJars(classes = {org.apache.http.client.ClientProtocolException.class,org.apache.http.HttpRequest.class})
public class MRJobStatusOperator implements Operator, IdleTimeHandler {
	private static final Logger LOG = LoggerFactory
			.getLogger(MRJobStatusOperator.class);

	/*
	 * each input string is of following format <uri>,<rm port>,<history server
	 * port>,<api version>,<hadoop version>,<application id>,<job id>
	 */
	public final transient DefaultInputPort<Map<String, String>> input = new DefaultInputPort<Map<String, String>>() {
		@Override
		public void process(Map<String, String> tuple) {

			if (jobMap == null) {
				jobMap = new HashMap<String, MRStatusObject>();
			}
			String s = null;
			String command = null;
			
			if(jobMap.size() >= maxMapSize)
				return;
			MRStatusObject mrStatusObj = new MRStatusObject();
			
			for (Map.Entry<String, String> e : tuple.entrySet()) {
				if (e.getKey().equals(Constants.QUERY_KEY_COMMAND)) {
					command = e.getValue();
				} else if (e.getKey().equals(Constants.QUERY_API_VERSION)) {
					mrStatusObj.setApiVersion(e.getValue());
				}else if(e.getKey().equals(Constants.QUERY_APP_ID)){
					mrStatusObj.setAppId(e.getValue());
				}else if(e.getKey().equals(Constants.QUERY_HADOOP_VERSION)){
					mrStatusObj.setHadoopVersion(Integer.parseInt(e.getValue()));
				}else if(e.getKey().equals(Constants.QUERY_HOST_NAME)){
					mrStatusObj.setUri(e.getValue());
				}else if(e.getKey().equals(Constants.QUERY_HS_PORT)){
					mrStatusObj.setHistoryServerPort(Integer.parseInt(e.getValue()));	
				}else if(e.getKey().equals(Constants.QUERY_JOB_ID)){
					mrStatusObj.setJobId(e.getValue());
				}else if(e.getKey().equals(Constants.QUERY_RM_PORT)){
					mrStatusObj.setRmPort(Integer.parseInt(e.getValue()));
				}
				
			}
			if ("delete".equalsIgnoreCase(command)) {
				removeJob(s);
				return;
			}
			
			if(jobMap.get(mrStatusObj.getJobId()) != null){
				mrStatusObj = jobMap.get(mrStatusObj.getJobId());
				
				output.emit(mrStatusObj.getJsonObject().toString());
				return;
			}
			
			if(mrStatusObj.getHadoopVersion() == 2){
				getJsonForJob(mrStatusObj);
			}else if(mrStatusObj.getHadoopVersion() ==1){
				getJsonForLegacyJob(mrStatusObj);
			}

			

		}
	};

	
	private transient Map<String, MRStatusObject> jobMap = new HashMap<String, MRStatusObject>();
	private transient int maxMapSize = Constants.MAX_MAP_SIZE;

	public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();
	public final transient DefaultOutputPort<String> mapOutput = new DefaultOutputPort<String>();
	public final transient DefaultOutputPort<String> reduceOutput = new DefaultOutputPort<String>();

	private void getJsonForJob(MRStatusObject statusObj) {

		String url = "http://" + statusObj.getUri() + ":"
				+ statusObj.getRmPort() + "/proxy/application_"
				+ statusObj.getAppId() + "/ws/v1/mapreduce/jobs/job_"
				+ statusObj.getJobId();
		String responseBody = Util.getJsonForURL(url);

		
		JSONObject jsonObj = getJsonObject(responseBody);

		if (jsonObj == null) {
			url = "http://" + statusObj.getUri() + ":"
					+ statusObj.getHistoryServerPort()
					+ "/ws/v1/history/mapreduce/jobs/job_"
					+ statusObj.getJobId();
			responseBody = Util.getJsonForURL(url);
		
			jsonObj = getJsonObject(responseBody);
		}

		if (jsonObj != null) {
			if (jobMap.get(statusObj.getJobId()) != null) {
				MRStatusObject tempObj = jobMap.get(statusObj.getJobId());
				if (tempObj.getJsonObject().toString().equals(jsonObj.toString()))
					return;
				// statusObj = tempObj;
			}

			output.emit(jsonObj.toString());
			statusObj.setJsonObject(jsonObj);
			getJsonsForTasks(statusObj);
			jobMap.put(statusObj.getJobId(), statusObj);
			iterator = jobMap.values().iterator();

		}
	}

	private void getJsonsForTasks(MRStatusObject statusObj) {
		String url = "http://" + statusObj.getUri() + ":"
				+ statusObj.getRmPort() + "/proxy/application_"
				+ statusObj.getAppId() + "/ws/v1/mapreduce/jobs/job_"
				+ statusObj.getJobId() + "/tasks/";
		String responseBody = Util.getJsonForURL(url);
		

		JSONObject jsonObj = getJsonObject(responseBody);
		if (jsonObj == null) {
			url = "http://" + statusObj.getUri() + ":"
					+ statusObj.getHistoryServerPort()
					+ "/ws/v1/history/mapreduce/jobs/job_"
					+ statusObj.getJobId() + "/tasks/";
			responseBody = Util.getJsonForURL(url);
			
			jsonObj = getJsonObject(responseBody);
		}

		if (jsonObj != null) {

			try{
			Map<String, JSONObject> mapTaskOject = statusObj.getMapJsonObject();
			Map<String, JSONObject> reduceTaskOject = statusObj
					.getReduceJsonObject();
			JSONArray taskJsonArray = jsonObj.getJSONObject("tasks").getJSONArray("task");

			for (int i = 0; i < taskJsonArray.length(); i++) {
				JSONObject taskObj = taskJsonArray.getJSONObject(i);
				if (Constants.REDUCE_TASK_TYPE.equalsIgnoreCase(taskObj.getString(Constants.TASK_TYPE))) {
					if (reduceTaskOject.get(taskObj.getString(Constants.TASK_ID)) != null) {
						JSONObject tempReduceObj = reduceTaskOject.get(taskObj.getString(Constants.TASK_ID));
						if (tempReduceObj.toString().equals(taskObj.toString()))
							continue;
					}
					reduceOutput.emit(taskObj.toString());
					reduceTaskOject.put(taskObj.getString(Constants.TASK_ID),
							taskObj);
				} else {
					if (mapTaskOject.get(taskObj.getString(Constants.TASK_ID)) != null) {
						JSONObject tempReduceObj = mapTaskOject.get(taskObj.getString(Constants.TASK_ID));
						if (tempReduceObj.toString().equals(taskObj.toString()))
							continue;
					}
					mapOutput.emit(taskObj.toString());

					mapTaskOject.put(taskObj.getString(Constants.TASK_ID),
							taskObj);
				}
			}
			statusObj.setMapJsonObject(mapTaskOject);
			statusObj.setReduceJsonObject(reduceTaskOject);
			}catch(Exception e){
				
			}
		}

	}

	
	private void getJsonForLegacyJob(MRStatusObject statusObj) {

		String url = "http://" + statusObj.getUri() + ":"
				+ statusObj.getRmPort()
				+ "/jobdetails.jsp?format=json&jobid=job_"
				+ statusObj.getJobId();
		String responseBody = Util.getJsonForURL(url);

		JSONObject jsonObj = getJsonObject(responseBody);
		if (jsonObj == null)
			return;

		if (jobMap.get(statusObj.getJobId()) != null) {
			MRStatusObject tempObj = jobMap.get(statusObj.getJobId());
			if (tempObj.getJsonObject().toString().equals(jsonObj.toString()))
				return;

		}

		output.emit(jsonObj.toString());
		statusObj.setJsonObject(jsonObj);
		getJsonsForLegacyTasks(statusObj, "map");
		getJsonsForLegacyTasks(statusObj, "reduce");
		jobMap.put(statusObj.getJobId(), statusObj);
		iterator = jobMap.values().iterator();

	}

	private void getJsonsForLegacyTasks(MRStatusObject statusObj, String type) {
		try{
		JSONObject jobJson = statusObj.getJsonObject();
		int totalTasks = ((JSONObject) ((JSONObject) jobJson.get(type
				+ "TaskSummary")).get("taskStats")).getInt("numTotalTasks");
		Map<String, JSONObject> taskMap;
		if (type.equalsIgnoreCase("map"))
			taskMap = statusObj.getMapJsonObject();
		else
			taskMap = statusObj.getReduceJsonObject();

		int totalPagenums = (totalTasks / Constants.MAX_TASKS) + 1;
		String baseUrl = "http://" + statusObj.getUri() + ":"
				+ statusObj.getRmPort() + "/jobtasks.jsp?type=" + type
				+ "&format=json&jobid=job_" + statusObj.getJobId()
				+ "&pagenum=";

		for (int pagenum = 1; pagenum <= totalPagenums; pagenum++) {

			String url=baseUrl+pagenum;
			String responseBody = Util.getJsonForURL(url);

			JSONObject jsonObj = getJsonObject(responseBody);
			if (jsonObj == null)
				return;

			JSONArray taskJsonArray = jsonObj.getJSONArray("tasksInfo");

			for (int i = 0; i < taskJsonArray.length(); i++) {
				JSONObject taskObj = taskJsonArray.getJSONObject(i);
				{
					if (taskMap.get(taskObj.getString(Constants.LEAGACY_TASK_ID)) != null) {
						JSONObject tempReduceObj = taskMap.get(taskObj
								.getString(Constants.LEAGACY_TASK_ID));
						if (tempReduceObj.toString().equals(taskObj.toString()))
							continue;
					}
					if (type.equalsIgnoreCase("map"))
						mapOutput.emit(taskObj.toString());
					else
						reduceOutput.emit(taskObj.toString());

					taskMap.put(taskObj.getString(Constants.LEAGACY_TASK_ID),
							taskObj);
				}
			}
		}

		if (type.equalsIgnoreCase("map"))
			statusObj.setMapJsonObject(taskMap);
		else
			statusObj.setReduceJsonObject(taskMap);
		}catch(Exception e){
		}

	}
	
	private JSONObject getJsonObject(String json) {
		return Util.getJsonObject(json);
	}

	private transient Iterator<MRStatusObject> iterator;

	@Override
	public void handleIdleTime() {
		if (!iterator.hasNext()) {
			iterator = jobMap.values().iterator();
		}

		if (iterator.hasNext()) {
			MRStatusObject obj = iterator.next();
			if(obj.getHadoopVersion() ==2)
				getJsonForJob(obj);
			else if(obj.getHadoopVersion() ==1)
				getJsonForLegacyJob(obj);
		}
	}

	@Override
	public void setup(OperatorContext arg0) {
		iterator = jobMap.values().iterator();
	}

	@Override
	public void teardown() {
	}

	@Override
	public void beginWindow(long arg0) {
	}

	@Override
	public void endWindow() {
	}

	public void removeJob(String jobId) {
		if (jobMap != null)
			jobMap.remove(jobId);
	}

	public int getMaxMapSize() {
		return maxMapSize;
	}

	public void setMaxMapSize(int maxMapSize) {
		this.maxMapSize = maxMapSize;
	}

}
