package com.datatorrent.mapreduce;

import java.net.URI;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.io.PubSubWebSocketInputOperator;
import com.datatorrent.lib.io.PubSubWebSocketOutputOperator;

public class MRDebuggerApplication implements StreamingApplication {

	private static final Logger LOG = LoggerFactory
			.getLogger(MRDebuggerApplication.class);

	@Override
	public void populateDAG(DAG dag, Configuration arg1) {
		String daemonAddress = dag.attrValue(DAG.DAEMON_ADDRESS, null);
		LOG.info(" gaurav reached here"+daemonAddress);
		LOG.debug(" gaurav reached here"+daemonAddress);
		if (daemonAddress== null || StringUtils.isEmpty(daemonAddress)) {
			daemonAddress = "10.0.2.15:9790";
		}
		MRJobStatusOperator mrJobOperator = dag.addOperator(
				"mrJobStatusOperator", new MRJobStatusOperator());
		
		
		URI uri = URI.create("ws://" + daemonAddress + "/pubsub");
		LOG.info("WebSocket with daemon at: {}", daemonAddress);

		PubSubWebSocketOutputOperator<Object> wsOut = dag.addOperator("mrDebuggerJobResultWS",new PubSubWebSocketOutputOperator<Object>());
		wsOut.setUri(uri);
		wsOut.setTopic("contrib.summit.mrDebugger.jobResult");
		
		PubSubWebSocketOutputOperator<Object> wsMapOut = dag.addOperator("mrDebuggerMapResultWS",new PubSubWebSocketOutputOperator<Object>());
		wsMapOut.setUri(uri);
		wsMapOut.setTopic("contrib.summit.mrDebugger.mapResult");
		
		PubSubWebSocketOutputOperator<Object> wsReduceOut = dag.addOperator("mrDebuggerReduceResultWS",new PubSubWebSocketOutputOperator<Object>());
		wsReduceOut.setUri(uri);
		wsReduceOut.setTopic("contrib.summit.mrDebugger.reduceResult");
		
		

		PubSubWebSocketInputOperator wsIn = dag.addOperator("mrDebuggerQueryWS", new PubSubWebSocketInputOperator());
		wsIn.setUri(uri);
		wsIn.addTopic("contrib.summit.mrDebugger.mrDebuggerQuery");

		dag.addStream("query", wsIn.outputPort, mrJobOperator.input);
		
		dag.addStream("jobConsoledata", mrJobOperator.output,wsOut.input);
		dag.addStream("mapConsoledata", mrJobOperator.mapOutput,wsMapOut.input);
		dag.addStream("reduceConsoledata", mrJobOperator.reduceOutput,wsReduceOut.input);
		

	}

}
