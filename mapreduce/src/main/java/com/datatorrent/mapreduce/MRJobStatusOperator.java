package com.datatorrent.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.IdleTimeHandler;
import com.datatorrent.api.Operator;

public class MRJobStatusOperator implements Operator,
		IdleTimeHandler {

	private static final Logger logger = LoggerFactory
			.getLogger(MRJobStatusOperator.class);

	/*
	 * each input string is of following format <uri>,<rm port>,<history server
	 * port>,<api version>,<hadoop version>,<application id>,<job id>
	 */
	public final transient DefaultInputPort<String> input = new DefaultInputPort<String>() {
		@Override
		public void process(String s) {

			if (jobMap == null) {
				jobMap = new ConcurrentHashMap<String, MRStatusObject>();
			}

			StringTokenizer tokenizer = new StringTokenizer(s, ",");
			if (tokenizer.countTokens() == 7
					&& jobMap.size() < Constants.MAX_MAP_SIZE) {
				MRStatusObject mrStatusObj = new MRStatusObject();
				mrStatusObj.setUri(tokenizer.nextToken());
				mrStatusObj.setRmPort(Integer.parseInt(tokenizer.nextToken()));
				mrStatusObj.setHistoryServerPort(Integer.parseInt(tokenizer
						.nextToken()));
				mrStatusObj.setApiVersion(tokenizer.nextToken());
				mrStatusObj.setHadoopVersion(tokenizer.nextToken());
				mrStatusObj.setAppId(tokenizer.nextToken());
				mrStatusObj.setJobId(tokenizer.nextToken());
				getJsonForJob(mrStatusObj);
			}

		}
	};
	private Map<String, MRStatusObject> jobMap = new ConcurrentHashMap<String, MRStatusObject>();

	public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();
	public final transient DefaultOutputPort<String> mapOutput = new DefaultOutputPort<String>();
	public final transient DefaultOutputPort<String> reduceOutput = new DefaultOutputPort<String>();

	private void getJsonForJob(MRStatusObject statusObj) {
		
		String url = "http://" + statusObj.getUri() + ":"
				+ statusObj.getRmPort() + "/proxy/application_"
				+ statusObj.getAppId() + "/ws/v1/mapreduce/jobs/job_"
				+ statusObj.getJobId();
		String responseBody = getJsonForURL(url);

		JSONObject jsonObj = getJsonObject(responseBody);
		
		if (jsonObj == null) {
			url = "http://" + statusObj.getUri() + ":"
					+ statusObj.getHistoryServerPort()
					+ "/ws/v1/history/mapreduce/jobs/job_"
					+ statusObj.getJobId();
			responseBody = getJsonForURL(url);
			jsonObj = getJsonObject(responseBody);
		}

		if (jsonObj != null) {			
			if (jobMap.get(statusObj.getJobId()) != null) {
				MRStatusObject tempObj = jobMap.get(statusObj.getJobId());
				if(tempObj.getJsonObject().equals(jsonObj))
					return;				
				//statusObj = tempObj;
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
		String responseBody = getJsonForURL(url);

		JSONObject jsonObj = getJsonObject(responseBody);
		if (jsonObj == null) {
			url = "http://" + statusObj.getUri() + ":"
					+ statusObj.getHistoryServerPort()
					+ "/ws/v1/history/mapreduce/jobs/job_"
					+ statusObj.getJobId() + "/tasks/";
			responseBody = getJsonForURL(url);
			jsonObj = getJsonObject(responseBody);
		}

		if (jsonObj != null) {
			Map<String, JSONObject> mapTaskOject = statusObj.getMapJsonObject();
			Map<String, JSONObject> reduceTaskOject = statusObj.getReduceJsonObject();
			JSONArray taskJsonArray = jsonObj.getJSONObject("tasks").getJSONArray("task");
			
			for (int i = 0; i < taskJsonArray.size(); i++) {
				JSONObject taskObj = taskJsonArray.getJSONObject(i);
				if (Constants.MAP_TASK_TYPE.equalsIgnoreCase(taskObj.getString(Constants.TASK_TYPE))) {
					if(reduceTaskOject.get(taskObj.getString(Constants.TASK_ID)) != null){
						JSONObject tempReduceObj = reduceTaskOject.get(taskObj.getString(Constants.TASK_ID));
						if(tempReduceObj.equals(taskObj))
							continue;
					}
					reduceOutput.emit(taskObj.toString());
					reduceTaskOject.put(taskObj.getString(Constants.TASK_ID),taskObj);
				} else {
					if(mapTaskOject.get(taskObj.getString(Constants.TASK_ID)) != null){
						JSONObject tempReduceObj = mapTaskOject.get(taskObj.getString(Constants.TASK_ID));
						if(tempReduceObj.equals(taskObj))
							continue;
					}
					mapOutput.emit(taskObj.toString());
					
					
					mapTaskOject.put(taskObj.getString(Constants.TASK_ID),taskObj);
				}
			}
			statusObj.setMapJsonObject(mapTaskOject);
			statusObj.setReduceJsonObject(reduceTaskOject);
		}

	}

	private JSONObject getJsonObject(String json) {
		try {
			JSONObject jsonObj = (JSONObject) JSONSerializer.toJSON(json);
			return jsonObj;
		} catch (Exception e) {
			logger.debug("{}", e.getMessage());
			return null;
		}
	}

	private String getJsonForURL(String url) {
		HttpClient httpclient = new DefaultHttpClient();
		logger.debug(url);
		try {

			// http://dataanalyser-virtualbox:8088/proxy/application_1376454776329_0001/ws/v1/mapreduce/jobs
			HttpGet httpget = new HttpGet(url);

			// Create a response handler
			ResponseHandler<String> responseHandler = new BasicResponseHandler();
			String responseBody;
			try {
				responseBody = httpclient.execute(httpget, responseHandler);
			} catch (ClientProtocolException e) {
				logger.debug(e.getMessage());
				return null;

			} catch (IOException e) {
				logger.debug(e.getMessage());
				return null;
			}catch(Exception e){
				logger.debug(e.getMessage());
				return null;
			}
			return responseBody;
		} finally {
			httpclient.getConnectionManager().shutdown();
		}
	}

	Iterator<MRStatusObject> iterator;
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

}
