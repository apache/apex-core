package com.datatorrent.mapreduce;

import java.io.IOException;

import org.codehaus.jettison.json.JSONObject;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Util {
	
	private static final Logger logger = LoggerFactory
			.getLogger(Util.class);
	
	public static String getJsonForURL(String url) {
		HttpClient httpclient = new DefaultHttpClient();
		logger.debug(url);
		try {

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
			return responseBody.trim();
		} finally {
			httpclient.getConnectionManager().shutdown();
		}
	}
	
	public static JSONObject getJsonObject(String json) {
		try {
			JSONObject jsonObj = new JSONObject(json);
			return jsonObj;
		} catch (Exception e) {
			logger.debug("{}", e.getMessage());
			return null;
		}
	}

}
