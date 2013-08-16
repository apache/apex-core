package com.datatorrent.mapreduce;

import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.IdleTimeHandler;
import com.datatorrent.api.Operator;

public class MRLegacyJobStatusOperator implements Operator, IdleTimeHandler {

	private Map<String, MRStatusObject> jobMap = new ConcurrentHashMap<String, MRStatusObject>();
	private Iterator<MRStatusObject> iterator;

	public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();
	public final transient DefaultOutputPort<String> mapOutput = new DefaultOutputPort<String>();
	public final transient DefaultOutputPort<String> reduceOutput = new DefaultOutputPort<String>();
	private int maxMapSize = Constants.MAX_MAP_SIZE;

	/*
	 * each input string is of following format <uri>,<job tracker port>,
	 * <hadoop version>,<job id>
	 */
	public final transient DefaultInputPort<String> input = new DefaultInputPort<String>() {
		@Override
		public void process(String s) {

			if (jobMap == null) {
				jobMap = new ConcurrentHashMap<String, MRStatusObject>();
			}

			StringTokenizer tokenizer = new StringTokenizer(s, ",");
			if (tokenizer.countTokens() == 4
					&& jobMap.size() < maxMapSize) {
				MRStatusObject mrStatusObj = new MRStatusObject();
				mrStatusObj.setUri(tokenizer.nextToken());
				mrStatusObj.setRmPort(Integer.parseInt(tokenizer.nextToken()));
				mrStatusObj.setHadoopVersion(tokenizer.nextToken());
				mrStatusObj.setJobId(tokenizer.nextToken());
				getJsonForJob(mrStatusObj);
			}

		}
	};

	private void getJsonForJob(MRStatusObject statusObj) {

		String url = "http://" + statusObj.getUri() + ":"
				+ statusObj.getRmPort()
				+ "/jobdetails.jsp?format=json&jobid=job_"
				+ statusObj.getJobId();
		String responseBody = Util.getJsonForURL(url);

		JSONObject jsonObj = Util.getJsonObject(responseBody);
		if (jsonObj == null)
			return;

		if (jobMap.get(statusObj.getJobId()) != null) {
			MRStatusObject tempObj = jobMap.get(statusObj.getJobId());
			if (tempObj.getJsonObject().equals(jsonObj))
				return;

		}

		output.emit(jsonObj.toString());
		statusObj.setJsonObject(jsonObj);
		getJsonsForTasks(statusObj, "map");
		getJsonsForTasks(statusObj, "reduce");
		jobMap.put(statusObj.getJobId(), statusObj);
		iterator = jobMap.values().iterator();

	}

	private void getJsonsForTasks(MRStatusObject statusObj, String type) {
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

			JSONObject jsonObj = Util.getJsonObject(responseBody);
			if (jsonObj == null)
				return;

			JSONArray taskJsonArray = jsonObj.getJSONArray("tasksInfo");

			for (int i = 0; i < taskJsonArray.size(); i++) {
				JSONObject taskObj = taskJsonArray.getJSONObject(i);
				{
					if (taskMap.get(taskObj
							.getString(Constants.LEAGACY_TASK_ID)) != null) {
						JSONObject tempReduceObj = taskMap.get(taskObj
								.getString(Constants.LEAGACY_TASK_ID));
						if (tempReduceObj.equals(taskObj))
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

	}

	@Override
	public void setup(OperatorContext arg0) {
		iterator = jobMap.values().iterator();
	}

	@Override
	public void teardown() {

	}

	@Override
	public void handleIdleTime() {
		if (!iterator.hasNext()) {
			iterator = jobMap.values().iterator();
		}

		if (iterator.hasNext()) {
			getJsonForJob(iterator.next());
		}
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
