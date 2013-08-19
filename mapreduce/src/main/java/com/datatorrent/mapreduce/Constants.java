package com.datatorrent.mapreduce;

public interface Constants {

	public final static int MAX_MAP_SIZE=25;
	
	public final static String REDUCE_TASK_TYPE="REDUCE";
	public final static String MAP_TASK_TYPE="MAP";
	public final static String TASK_TYPE="type";
	public final static String TASK_ID="id";
	
	public final static String LEAGACY_TASK_ID="taskId";
	public final static int MAX_TASKS = 2000;
	
	public final static String QUERY_APP_ID="app_id";
	public final static String QUERY_JOB_ID="job_id";
	public final static String QUERY_HADOOP_VERSION="hadoop_version";
	public final static String QUERY_API_VERSION="api_version";
	public final static String QUERY_RM_PORT="rm_port";
	public final static String QUERY_HS_PORT="hs_port";
	public final static String QUERY_HOST_NAME="hostname";
	public static final String QUERY_KEY_COMMAND = "command";
	
}
