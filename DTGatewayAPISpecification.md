# DT Gateway API v2 Specification

# REST API

## Return codes

200: OK
400: The request is not in the format that the server expects
404: The resource is not found
500: Something is wrong on the server side

## REST URI Specification
### GET /ws/v2/about

Function:
Return:

```json
{
    "buildVersion": "{buildVersion}",
    "buildDate": "{date and time}",
    "buildRevision": "{revision}",
    "buildUser": "{user}",
    "version": "{version}",
    "gatewayUser": "{user}",
    "javaVersion": "{java\_version}",
    "hadoopLocation": "{hadoop\_location}",
    "jvmName": "{pid}@{hostname}",
    "configDirectory": "{configDir}",
    “hostname”: “{hostname}”,
    “hadoopIsSecurityEnabled”: “{true/false}”
}
```

### GET /ws/v2/cluster/metrics

Function: List metrics that are relevant to the entire cluster
Return:

```json
{
    "averageAge": "{average running application age in milliseconds}",
    "cpuPercentage": "{cpuPercentage}",
    "currentMemoryAllocatedMB": "{currentMemoryAllocatedMB}",
    "maxMemoryAllocatedMB": "{maxMemoryAllocatedMB}",
    "numAppsFailed": "{numAppsFailed}",
    "numAppsFinished": "{numAppsFinished}",
    "numAppsKilled": "{numAppsKilled}",
    "numAppsPending": "{numAppsPending}",
    "numAppsRunning": "{numAppsRunning}",
    "numAppsSubmitted": "{numAppsSubmitted}",
    "numContainers": "{numContainers}",
    "numOperators": "{numOperators}",
    "tuplesEmittedPSMA": "{tuplesEmittedPSMA}",
    "tuplesProcessedPSMA": "{tuplesProcessedPSMA}"
}
```

### GET /ws/v2/applications[?states={STATE\_FILTER}&name={NAME\_FILTER}&user={USER\_FILTER]

Function: List IDs of all streaming applications
Return:

```json
{
    "apps": [
        {
            "diagnostics": "{diagnostics}",
            "elapsedTime": "{elapsedTime}",
            "finalStatus": "{finalStatus}",
            "finishedTime": "{finishedTime}",
            "id": "{appId}",
            "name": "{name}",
            "queue": "{queue}",
            "startedTime": "{startedTime}",
            "state": "{state}",
            "trackingUrl": "{trackingUrl}",
            "user": "{user}"
        },  
        …
    ]
}
```        

### GET /ws/v2/applications/{appid}

Function: Get the information for the specified application
Return:

```json
{
    "id": "{appid}",
    "name": "{name}",
    "state": {state}
    "trackingUrl": "{tracking url}",
    "finalStatus": {finalStatus},
    "appPath": "{appPath}",
    "gatewayAddress": "{gatewayAddress}",
    "elapsedTime": "{elapsedTime}",
    "startedTime": "{startTime}",
    "user": "{user}",
    "version": "{stram version}",
    "remainingLicensedMB": "{remainingLicensedMB}",
    "allocatedMB": "{allocatedMB}",
    "gatewayConnected": true/false,
    "connectedToThisGateway": true/false,
    "attributes": {
           "{attributeName}": {attributeValue}, ...
    }
    "stats": {
        "allocatedContainers": {allocatedContainer},
        "totalMemoryAllocated": {totalMemoryAllocated},
        "latency": {overall latency},
        "criticalPath": {list of operator id that represents the
critical path},
        "failedContainers": {failedContainers},
        "numOperators": {numOperators},
        "plannedContainers": {plannedContainers},
        "currentWindowId":{min of operators:currentWindowId},
        "recoveryWindowId":{min of operators:recoveryWindowId},
        "tuplesProcessedPSMA":{sum of operators:tuplesProcessedPSMA},
        "totalTuplesProcessed":{sum of operators:totalTuplesProcessed},
        "tuplesEmittedPSMA":{sum of operators:tuplesEmittedPSMA},
        "totalTuplesEmitted":{sum of operators:totalTuplesEmitted},
        "totalBufferServerReadBytesPSMA":
{totalBufferServerReadBytesPSMA},
        "totalBufferServerWriteBytesPSMA":
{totalBufferServerWriteBytesPSMA}
    }
}
```

### GET /ws/v2/applications/{appid}/physicalPlan

Function: Return the physical plan for the given application
Return:

```json
{
    "operators": [
        {
            "className": "{className}",
            "container": "{containerId}",
            "cpuPercentageMA": "{cpuPercentageMA}",
            "currentWindowId": "{currentWindowId}",
            "failureCount": "{failureCount}",
            "host": "{host}",
            "id": "{id}",
            "ports": [
                {
                    "bufferServerBytesPSMA": "{bufferServerBytesPSMA}",
                    "name": "{name}",
                    "totalTuples": "{totalTuples}",
                    "tuplesPSMA": "{tuplesPSMA}",
                    "type": "input/output",
                    "recordingStartTime": "{recordingStartTime}"
                },
                ...
            ],
            "lastHeartbeat": "{lastHeartbeat}",
            "latencyMA": "{latencyMA}",
            "name": "{name}",
            "recordingStartTime": "{recordingStartTime}",
            "recoveryWindowId": "{recoveryWindowId}",
            "status": "{status}",
            "totalTuplesEmitted": "{totalTuplesEmitted}",
            "totalTuplesProcessed": "{totalTuplesProcessed}",
            "tuplesEmittedPSMA": "{tuplesEmittedPSMA}",
            "tuplesProcessedPSMA": "{tuplesProcessedPSMA}",
            "logicalName": "{logicalName}",
            "isUnifier": true/false
        },
         …
     ],
     "streams": [        
        {
            "logicalName": "{logicalName}",
            "sinks": [
                {
                    "operatorId": "{operatorId}",
                    "portName": "{portName}"
                }, ...
            ],
            "source": {
                "operatorId": "{operatorId}",
                "portName": "{portName}"
            },
            "locality": "{locality}"
        }, ...
     ]
}
```

        
### GET /ws/v2/applications/{appid}/physicalPlan/operators

Function: Return list of operators for the given application
Return:

```json
{
    "operators": [
        {
            "className": "{className}",
            "container": "{containerId}",
                      “counters”: {
                “{counterName}: “{counterValue}”, ...            
             }
            "cpuPercentageMA": "{cpuPercentageMA}",
            "currentWindowId": "{currentWindowId}",
            "failureCount": "{failureCount}",
            "host": "{host}",
            "id": "{id}",
            "ports": [
                {
                    "bufferServerBytesPSMA": "{bufferServerBytesPSMA}",
                    "name": "{name}",
                    "totalTuples": "{totalTuples}",
                    "tuplesPSMA": "{tuplesPSMA}",
                    "type": "input/output",
                    "recordingStartTime": "{recordingStartTime}"
                },
                ...
            ],
            "lastHeartbeat": "{lastHeartbeat}",
            "latencyMA": "{latencyMA}",
            "name": "{name}",
            "recordingStartTime": "{recordingStartTime}",
            "recoveryWindowId": "{recoveryWindowId}",
            "status": "{status}",
            "totalTuplesEmitted": "{totalTuplesEmitted}",
            "totalTuplesProcessed": "{totalTuplesProcessed}",
            "tuplesEmittedPSMA": "{tuplesEmittedPSMA}",
            "tuplesProcessedPSMA": "{tuplesProcessedPSMA}",
            "logicalName": "{logicalName}",
            "unifierClass": "{unifierClass}"
        },
         …
     ]
}
```

### GET /ws/v2/applications/{appid}/physicalPlan/streams

Function: Return physical streams
Return:

```json
{
     "streams": [        
        {
            "logicalName": "{logicalName}",
            "sinks": [
                {
                    "operatorId": "{operatorId}",
                    "portName": "{portName}"
                }, ...
            ],
            "source": {
                "operatorId": "{operatorId}",
                "portName": "{portName}"
            },
            "locality": "{locality}"
        }, ...
     ]
}
```

### GET /ws/v2/applications/{appid}/physicalPlan/operators/{opid}

Function: Return information of the given operator for the given
application
Return:

```json
{
    "className": "{className}",
    "container": "{containerId}",
    “counters”: {
      “{counterName}: “{counterValue}”, ...            
    }
    "cpuPercentageMA": "{cpuPercentageMA}",
    "currentWindowId": "{currentWindowId}",
    "failureCount": "{failureCount}",
    "host": "{host}",
    "id": "{id}",
    "ports": [
       {
          "bufferServerBytesPSMA": "{bufferServerBytesPSMA}",
          "name": "{name}",
          "totalTuples": "{totalTuples}",
          "tuplesPSMA": "{tuplesPSMA}",
          "type": "input/output"
       }, ...
    ],
    "lastHeartbeat": "{lastHeartbeat}",
    "latencyMA": "{latencyMA}",
    "name": "{name}",
    "recordingStartTime": "{recordingStartTime}",
    "recoveryWindowId": "{recoveryWindowId}",
    "status": "{status}",
    "totalTuplesEmitted": "{totalTuplesEmitted}",
    "totalTuplesProcessed": "{totalTuplesProcessed}",
    "tuplesEmittedPSMA": "{tuplesEmittedPSMA}",
    "tuplesProcessedPSMA": "{tuplesProcessedPSMA}"
}
```

### GET /ws/v2/applications/{appid}/physicalPlan/operators/{opid}/deployHistory

Function: Return container deploy history of this operator
Since: 1.0.6
Return:

```json
{
   "containers": [  
        {  
            "container": "{containerId}",   
            "startTime": "{startTime}"  
        }, ...  
    ],   
    "name": "{operatorName}"
}
```

### GET /ws/v2/applications/{appid}/physicalPlan/operators/{opid}/ports

Function: Get the information of all ports of the given operator of the
given application
Return:

```json
{  
    "ports": [
        {  
            "bufferServerBytesPSMA": "{bufferServerBytesPSMA}",   
            "name": "{name}",
            "recordingStartTime": "{recordingStartTime}",  
            "totalTuples": "{totalTuples}",   
            "tuplesPSMA": "{tuplesPSMA}",   
            "type": "output"  
        }, …
    ]
}
```  

### GET /ws/v2/applications/{appid}/physicalPlan/operators/{opid}/ports/{portName}

Function: Get the information of a specified port
Return:

```json
{  
    "bufferServerBytesPSMA": "{bufferServerBytesPSMA}",   
    "name": "{name}",   
    "totalTuples": "{totalTuples}",   
    "tuplesPSMA": "{tuplesPSMA}",   
    "type": "{type}"  
}
```

### GET /ws/v2/applications/{appid}/operatorClasses[?parent={parent}&q={searchTerm}&packagePrefixes={comma-separated-package-prefixes}]

Function: Get the classes of operators, if given the parent parameter,
all classes that inherits from parent
Return:

```json
{  
    "operatorClasses": [  
        { "name":"{className}" },
       …
     ]
}
```

### GET /ws/v2/applications/{appid}/operatorClasses/{operatorClass}

Function: Get the description of the given operator class
Return:

```json
{
    "inputPorts": [
        {
            "name": "{name}",
            "optional": {boolean}
        },
          ...
    ],
    "outputPorts": [
        {
            "name": "{name}",
            "optional": {boolean}
        },
        …
    ],
    "properties": [  
        {
          "name":"{className}",
          "canGet": {canGet},
          "canSet": {canSet},
          "type":"{type}",
          "description":"{description}",
          "properties": ...
        },
       …
     ]
}
```

### POST /ws/v2/applications/{appid}/shutdown

Function: Shut down the application
Payload: none
### POST /ws/v2/applications/{appid}/kill

Function: Kill the given application
Payload: none
### POST /ws/v2/applications/{appid}/physicalPlan/operators/{opid}/recordings/start

Function: Start recording on operator
Payload (optional):

```json
{
   "numWindows": {number of windows to record}  (if not given, the
recording goes on forever)
}
```

Returns:

```json
{
    "id": "{recordingId}",
}
```

### POST /ws/v2/applications/{appid}/physicalPlan/operators/{opid}/recordings/stop

Function: Stop recording on operator
Payload: none
### POST /ws/v2/applications/{appid}/physicalPlan/operators/{opid}/ports/{portName}/recordings/start

Function: Start recording on port
Payload (optional):

```json
{
   "numWindows": {number of windows to record}  (if not given, the
recording goes on forever)
}
```

Returns:

```json
{
    "id": "{recordingId}",
}
```

### POST /ws/v2/applications/{appid}/physicalPlan/operators/{opid}/ports/{portName}/recordings/stop

Function: Stop recording on port
Payload: none
### GET /ws/v2/applications/{appid}/physicalPlan/containers[?states={NEW,ALLOCATED,ACTIVE,KILLED}]

Function: Return the list of containers for this application
Return:

```json
{
    "containers": [
        {
            "host": "{host}",
            "id": "{id}",
            "jvmName": "{jvmName}",
            "lastHeartbeat": "{lastHeartbeat}",
            "memoryMBAllocated": "{memoryMBAllocated}",
            "memoryMBFree": "{memoryMBFree}",
            "numOperators": "{numOperators}",
            "containerLogsUrl": "{containerLogsUrl}",
            "state": "{state}"
        }, …
    ]
}
```

### GET /ws/v2/applications/{appid}/physicalPlan/containers/{containerId}

Function: Return the information of the specified container
Return:

```json
{
    "host": "{host}",
    "id": "{id}",
    "jvmName": "{jvmName}",
    "lastHeartbeat": "{lastHeartbeat}",
    "memoryMBAllocated": "{memoryMBAllocated}",
    "memoryMBFree": "{memoryMBFree}",
    "numOperators": "{numOperators}",
    "containerLogsUrl": "{containerLogsUrl}",
    "state": "{state}"
}
```

### GET /ws/v2/applications/{appid}/physicalPlan/containers/{containerId}/logs

Function: Return the container log list
Return:

```json
{
    "logs": [
        {
            "length": "{log length}",
            "name": "{logName}"
        }, ...
    ]
}
```

### GET /ws/v2/applications/{appid}/physicalPlan/containers/{containerId}/logs/{logName}[?start={startPos}&end={endPos}&grep={regexp}&includeOffset={true/false}]

Function: Return the raw log
Return: if includeOffset=false or not provided, return raw log content
(Content-Type: text/plain). Otherwise (Content-Type: application/json):

```json
{
    "lines": [
        { "byteOffset":"{byteOffset}", "line": "{line}" }, …
     ]
}
```

### POST /ws/v2/applications/{appid}/physicalPlan/containers/{containerId}/kill

Function: Kill this container
Payload: none
### GET /ws/v2/applications/{appid}/logicalPlan

Function: Return the logical plan of this application
Return:

```json
{
    "operators": [
      {
        "name": "{name}",
        "attributes": {attributeMap},
        "class": "{class}",
        "ports": {
           [
            {
                "name": "{name}",
                "attributes": {attributeMap},
                "type": "input/output"
            }, ...
           ]
         },
         "properties": {
            "class": "{class}"
         }
      }, ...
    ],
    "streams": [
        {
            "name": "{name}",
            "locality": "{locality}",
            "sinks": [
                {
                    "operatorName": "{operatorName}",
                    "portName": "{portName}"
                }, ...
            ],
            "source": {
                "operatorName": "{operatorName}",
                "portName": "{portName}"
            }
        }, ...
    ]
}
```

### GET /ws/v2/applications/{appid}/logicalPlan/attributes

Function: Return the application attributes
Return:

```json
{
    "{name}": value, ...
}
```

### GET /ws/v2/applications/{appid}/logicalPlan/operators

Function: Return the list of info of the logical operator
Return:

```json
{
    "operators": [
        {
            "className": "{className}",
            "containerIds": [ "{containerid}", … ],
            "cpuPercentageMA": "{cpuPercentageMA}",
            "currentWindowId": "{currentWindowId}",
            "failureCount": "{failureCount}",
            "hosts": [ "{host}", … ],
            "lastHeartbeat": "{lastHeartbeat}",
            "latencyMA": "{latencyMA}",
            "name": "{name}",
            "partitions": [ "{operatorid}", … ],
            "recoveryWindowId": "{recoveryWindowId}",
            "status": {
                "{state}": "{number}", ...
            },
            "totalTuplesEmitted": "{totalTuplesEmitted}",
            "totalTuplesProcessed": "{totalTuplesProcessed}",
            "tuplesEmittedPSMA": "{tuplesEmittedPSMA}",
            "tuplesProcessedPSMA": "{tuplesProcessedPSMA}",
            "unifiers": [ "{operatorid}", … ],
            “counters”: {
                 “{counterName}: {
                    “avg”: …, “max”: …, “min”: …, “sum”: ...
                 }
            }
        }, ...
    ]
}
```

### GET /ws/v2/applications/{appid}/logicalPlan/operators/{opName}

Function: Return the info of the logical operator
Return:

```json
{
            "className": "{className}",
            "containerIds": [ "{containerid}", … ],
            "cpuPercentageMA": "{cpuPercentageMA}",
            "currentWindowId": "{currentWindowId}",
            "failureCount": "{failureCount}",
            "hosts": [ "{host}", … ],
            "lastHeartbeat": "{lastHeartbeat}",
            "latencyMA": "{latencyMA}",
            "name": "{name}",
            "partitions": [ "{operatorid}", … ],
            "recoveryWindowId": "{recoveryWindowId}",
            "status": {
                "{state}": "{number}", ...
            },
            "totalTuplesEmitted": "{totalTuplesEmitted}",
            "totalTuplesProcessed": "{totalTuplesProcessed}",
            "tuplesEmittedPSMA": "{tuplesEmittedPSMA}",
            "tuplesProcessedPSMA": "{tuplesProcessedPSMA}",
            "unifiers": [ "{operatorid}", … ],
            “counters”: {
                 “{counterName}: {
                    “avg”: …, “max”: …, “min”: …, “sum”: ...
                 }
            }
}
```

### GET /ws/v2/applications/{appid}/logicalPlan/operators/{opName}/properties

Function: Return the properties of the logical operator
Return:

```json
{
    "{name}": value, ...
}
```

### POST /ws/v2/applications/{appid}/logicalPlan/operators/{opName}/properties

Function: Set the properties of the logical operator
Payload:

```json
{
    "{name}": value, ...
}
```

### GET /ws/v2/applications/{appid}/physicalPlan/operators/{opId}/properties

Function: Return the properties of the physical operator
Return:

```json
{
    "{name}": value, ...
}
```

### POST /ws/v2/applications/{appid}/physicalPlan/operators/{opId}/properties

Function: Set the properties of the physical operator
Payload:

```json
{
    "{name}": value, ...
}
```

### GET /ws/v2/applications/{appid}/logicalPlan/operators/{opName}/attributes

Function: Get the attributes of the logical operator
Return:

```json
{
    "{name}": value, ...
}
```

### GET /ws/v2/applications/{appid}/logicalPlan/operators/{opName}/ports/{portName}/attributes

Function:  Get the attributes of the port
Return:

```json
{
    "{name}": value, ...
}
```

### POST /ws/v2/applications/{appid}/logicalPlan

Function: Change logical plan of this application
Payload:

```json
{
    "requests": [
        {
            "requestType": "AddStreamSinkRequest",
            "streamName": "{streamName}",
            "sinkOperatorName": "{sinkOperatorName}",
            "sinkOperatorPortName": "{sinkOperatorPortName}"
        },
        {
            "requestType": "CreateOperatorRequest",
            "operatorName": "{operatorName}",
            "operatorFQCN": "{operatorFQCN}",
        },
        {
            "requestType": "CreateStreamRequest",
            "streamName": "{streamName}",
            "sourceOperatorName": "{sourceOperatorName}",
            "sourceOperatorPortName": "{sourceOperatorPortName}"
            "sinkOperatorName": "{sinkOperatorName}",
            "sinkOperatorPortName": "{sinkOperatorPortName}"
        },
        {
            "requestType": "RemoveOperatorRequest",
            "operatorName": "{operatorName}",
        },
        {
            "requestType": "RemoveStreamRequest",
            "streamName": "{streamName}",
        },
        {
            "requestType": "SetOperatorPropertyRequest",
            "operatorName": "{operatorName}",
            "propertyName": "{propertyName}",
            "propertyValue": "{propertyValue}"
        },
        ...
    ]
}
```

### GET /ws/v2/applications/{appid}/logicalPlan/operators/{opName}/stats/meta

Function: Return the meta information about the statistics stored for
this operator
Return:

```json
{
    "appId": "{appId}",
    "operatorName": "{operatorName}",
    "operatorIds": [ {opid}, … ]
    "startTime": "{startTime}"
    "endTime": "{endTime}"
    "count": "{count}"
    "ended": {boolean}
}
```

### GET /ws/v2/applications/{appid}/logicalPlan/operators/{opName}/stats?startTime={startTime}&endTime={endTime}

Function: Return the statistics stored for this logical operator

```json
{
    "operatorStats": [
        {
            "operatorId": "{operatorId}"
            "timestamp": "{timestamp}"
            "stats": {
                "container": "containerId",
                "host": "host",
                "totalTuplesProcessed", "{totalTuplesProcessed}",
                "totalTuplesEmitted", "{totalTuplesEmitted}",
                "tuplesProcessedPSMA", "{tuplesProcessedPSMA}",
                "tuplesEmittedPSMA": "{tuplesEmittedPSMA}",
                "cpuPercentageMA": "{cpuPercentageMA}",
                "latencyMA": "{latencyMA}",
                "ports": [ {
                    "name": "{name}",
                    "type":"{input/output}",
                    "totalTuples": "{totalTuples}",
                    "tuplesPSMA", "{tuplesPSMA}",
                    "bufferServerBytesPSMA", "{bufferServerBytesPSMA}"
                }, … ],
            }
        }, ...
    ]
}
```

### GET /ws/v2/applications/{appid}/physicalPlan/containers/stats/meta

Function: Return the meta information about the container statistics

```json
{
    "appId": "{appId}",
    "containers": {
        "{containerId}": {
            "id": "{id}",
            "jvmName": "{jvmName}",
            "host": "{host}",
            "memoryMBAllocated", "{memoryMBAllocated}"
        }
        …
    }
    "startTime": "{startTime}"
    "endTime": "{endTime}"
    "count": "{count}"
    "ended": {boolean}
}
```

### GET /ws/v2/applications/{appid}/physicalPlan/containers/stats?startTime={startTime}&endTime={endTime}

Function: Return the container statistics stored for this application

```json
{
    "containerStats": [
        {
            "containerId": "{containerId}"
            "timestamp": "{timestamp}"
            "stats": {
                "numOperators": "{numOperators}",
            }
        }, ...
    ]
}
```

### GET /ws/v2/applications/{appid}/recordings

Function: Get the list of all recordings for this application
Return:

```json
{
    "recordings": [{
        "id": "{id}",
        "startTime": "{startTime}",
        "appId": "{appId}",
        "operatorId": "{operatorId}",
        "containerId": "{containerId}",
        "totalTuples": "{totalTuples}",
        "ports": [ {
            "name": "{portName}",
            "streamName": "{streamName}",
            "type": "{type}",
            "id": "{index}",
            "tupleCount": "{tupleCount}"
        } … ],
        "ended": {boolean},
        "windowIdRanges": [ {
            "low": "{lowId}",
            "high": "{highId}"
        } … ],
        "properties": {
            "name": "value", ...
        }
    }, ...]
}
```

### GET /ws/v2/applications/{appid}/physicalPlan/operators/{opid}/recordings

Function: Get the list of recordings on this operator
Return:

```json
{
    "recordings": [ {
        "id": "{id}",
        "startTime": "{startTime}",
        "appId": "{appId}",
        "operatorId": "{operatorId}",
        "containerId": "{containerId}",
        "totalTuples": "{totalTuples}",
        "ports": [ {
            "name": "{portName}",
            "streamName": "{streamName}",
            "type": "{type}",
            "id": "{index}",
            "tupleCount": "{tupleCount}"
        } … ],
        "ended": {boolean},
        "windowIdRanges": [ {
            "low": "{lowId}",
            "high": "{highId}"
        } … ],
        "properties": {
            "name": "value", ...
        }
    }, ...]
}
```

### GET /ws/v2/applications/{appid}/physicalPlan/operators/{opid}/recordings/{id}

Function: Get the information about the recording
Return:

```json
{
    "id": "{id}",
    "startTime": "{startTime}",
    "appId": "{appId}",
    "operatorId": "{operatorId}",
    "containerId": "{containerId}",
    "totalTuples": "{totalTuples}",
    "ports": [ {
       "name": "{portName}",
       "streamName": "{streamName}",
       "type": "{type}",
       "id": "{index}",
       "tupleCount": "{tupleCount}"
     } … ],
    "ended": {boolean},
    "windowIdRanges": [ {
       "low": "{lowId}",
       "high": "{highId}"
     } … ],
    "properties": {
       "name": "value", ...
     }
}
```

### DELETE /ws/v2/applications/{appid}/physicalPlan/operators/{opid}/recordings/{id}

Function: Deletes the specified recording
Since: 1.0.4
### GET /ws/v2/applications/{appid}/physicalPlan/operators/{opid}/recordings/{id}/tuples

Query Parameters:
        offset
        startWindow
        limit
        ports
        executeEmptyWindow
Function: Get the tuples
Return:

```json
{
    "startOffset": "{startOffset}"
    "tuples": [ {
        "windowId": "{windowId}"
        "tuples": [ {
            "portId": "{portId}"
            "data": {tupleData}
        }, … ]
    }, … ]
}
```

### GET /ws/v2/applications/{appid}/events?from={fromTime}&to={toTime}&offset={offset}&limit={limit}

Function: Get the events
Return:

```json
{
    "events": [ {
           "id": "{id}",
        "timestamp": "{timestamp}",
        "type": "{type}",
        "data": {
            "name": "value", …
        }
    }, … ]
}
```

### GET /ws/v2/profile/user

Function: Get the user profile information, list of roles and list of
permissions given the user
Return:

```json
{
    "authScheme": "{authScheme}",
    "userName" : "{userName}",
    "roles": [ "{role1}", … ],
    "permissions": [ "{permission1}", … ]
}
```

### GET /ws/v2/profile/settings

Function: Get the current user's settings
Return:

```json
{
    "{key}": {value}, ...
}
```

### GET /ws/v2/profile/settings/{user}

Function: Get the specified user's settings
Return:

```json
{
    "{key}": {value}, ...
}
```

### GET /ws/v2/profile/settings/{user}/{key}

Function: Get the specified user's setting key
Return:

```json
{
    "value": {value}
}
```

### PUT /ws/v2/profile/settings/{user}/{key}

Function: Set the specified user's setting key
Payload:

```json
{
    "value": {value}
}
```

### GET /ws/v2/auth/roles

Function: Get the list of roles the system has
Return:

```json
{
    "roles": [
       {
         "name": "{role1}",
         "permissions": [ "{permission1}", … ]
       }, …
    ]
}
```

### GET /ws/v2/auth/roles/{role}

Function: Get the list of permissions given the role
Return:

```json
{
    "permissions": [ "{permissions1}", … ]
}
```

### PUT /ws/v2/auth/roles/{role}

Function: create or edit the list of permissions given the role
Return:

```json
{
    "permissions": [ "{permissions1}", … ]
}
```

### POST /ws/v2/auth/restoreDefaultRoles

Function: Restores default roles
### DELETE /ws/v2/auth/roles/{role}

Function: delete the given role
### GET /ws/v2/auth/permissions

Function: Get the list of possible permissions
Return:

```json
{
    "permissions": [ {
       "name": "{permissionName}",
       "adminOnly": true/false
    }, … ]
}
```

### PUT /ws/v2/applications/{appid}/permissions

Function: Set the permissions details for this application
Payload:

```json
{
        "readOnly": {
"roles": [ "role1", … ],
"users": [ "user1", … ],
"everyone": true/false
}
```,

"readWrite": {
"roles": [ "role1", … ],
"users": [ "user1", … ],
"everyone": true/false
}
}
### GET /ws/v2/applications/{appid}/permissions

Function: Get the permissions details for this application
Return:

```json
{
        "readOnly": {
"roles": [ "role1", … ],
"users": [ "user1", … ],
"everyone": true/false
}
```,

"readWrite": {
"roles": [ "role1", … ],
"users": [ "user1", … ],
"everyone": true/false
}
}
### PUT /ws/v2/appPackages/{owner}/{name}/permissions

Function: Set the permissions details for this application
Payload:

```json
{
        "readOnly": {
"roles": [ "role1", … ],
"users": [ "user1", … ],
"everyone": true/false
}
```,

"readWrite": {
"roles": [ "role1", … ],
"users": [ "user1", … ],
"everyone": true/false
}
}
### GET /ws/v2/appPackages/{owner}/{name}/permissions

Function: Get the permissions details for this application
Return:

```json
{
        "readOnly": {
"roles": [ "role1", … ],
"users": [ "user1", … ],
"everyone": true/false
}
```,

"readWrite": {
"roles": [ "role1", … ],
"users": [ "user1", … ],
"everyone": true/false
}
}
### POST /ws/v2/licenses

Function: Add a license to the registry or generate an eval license
Payload: The license file content, if payload is empty, it will try to
generate an eval license and return the info
Return:

```json
{
  “id”: “{licenseId}”,
  “expireTime”: {unixTimeMillis},
  “nodesAllowed”: {nodesAllowed},
  “memoryMBAllowed”: {memoryMBAllowed},
  “contextType”: “{contextType}”,
  “type”: “{type}”,
  “features”: [ “{feature1}”, … ]
}
```

### GET /ws/v2/licenses/current

Function: Get info on the current license

```json
{
      “id”: “{licenseId}”,
      “expireTime”: {unixTimeMillis},
      “nodesAllowed”: {nodesAllowed},
      “nodesUsed”: {nodesUsed},
      “memoryMBAllowed”: {memoryMBAllowed},
      “memoryMBUsed”: {memoryMBUsed},
      “contextType”: “{community|standard|enterprise}”,
      “type”: “{evaluation|non\_production|production}”
      “features”: [ “{feature1}”, … ], // for community, empty array
      “current”: true/false
}
```

### GET /ws/v2/config/installMode

Function: returns the install mode

```json
{
  “installMode”: “{evaluation|community|app}”,
  “appPackageName”: “{optionalAppPackageName}”,
  “appPackageVersion”: “{optionalAppPackageVersion}”
}
```

### GET /ws/v2/config/properties/dt.phoneHome.enable

Function: returns the download type

```json
{
  “value”: “true/false”
}
```

### PUT /ws/v2/config/properties/dt.phoneHome.enable

Function:

```json
{
  “value”: “true/false”
}
```

Feature List:
  SYSTEM\_APPS, SYSTEM\_ALERTS, APP\_DATA\_DASHBOARDS,
RUNTIME\_DAG\_CHANGE, RUNTIME\_PROPERTY\_CHANGE, APP\_CONTAINER\_LOGS,
LOGGING\_LEVELS, APP\_DATA\_TRACKER, JAAS\_LDAP\_AUTH, APP\_BUILDER
### 

### GET /ws/v2/config/properties

Function: Returns list of properties from dt-site.xml.
Return:

```json
{
"{name}": {
"value": "{PROPERTY\_VALUE}",
        "description": "{PROPERTY\_DESCRIPTION}"
}
```, ...

}
### GET /ws/v2/config/properties/{PROPERTY\_NAME}

Function: Returns single property from dt-site.xml, specify by name
Return:

```json
{
"value": "{PROPERTY\_VALUE}",
"description": "{PROPERTY\_DESCRIPTION}"
}
```

### POST /ws/v2/config/properties

Function: Overwrites all specified properties in dt-site.xml
Payload:

```json
{
"properties": [
  {
        “name”: “{name}”
"value": "{PROPERTY\_VALUE}",
“local”: true/false,
        "description": "{PROPERTY\_DESCRIPTION}"
  }, …
]
}
```

### PUT /ws/v2/config/properties/{PROPERTY\_NAME}

Function: Overwrites or creates new property in dt-site.xml
Payload:

```json
{
"value": "{PROPERTY\_VALUE}",
“local”: true/false,
"description": "{PROPERTY\_DESCRIPTION}"
}
```

### DELETE /ws/v2/config/properties/{PROPERTY\_NAME}

Function: Deletes a property from dt-site.xml. This may have to be
restricted to custom properties?
### GET /ws/v2/config/hadoopExecutable

Function: Returns the hadoop executable
Return:

```json
{
"value": "{PROPERTY\_VALUE}",
}
```

### PUT /ws/v2/config/hadoopExecutable

Function: Sets the hadoop executable
Return:

```json
{
"value": "{PROPERTY\_VALUE}",
}
```

### GET /ws/v2/config/issues

Function: Returns list of potential issues with environment
Return:

```json
{
"issues": [
        {
                "key": "{issueKey}",
                "propertyName": "{PROPERTY\_NAME}",
                "description": "{ISSUE\_DESCRIPTION}",
                "severity": "error"|"warning"
        },
        {...},
        {...}
]    
}
```

### GET /ws/v2/config/ipAddresses

Function: Returns list of ip addresses the gateway can listen to
Return:

```json
{
"ipAddresses": [
  "1.2.3.4", ...
]    
}
```

### POST /ws/v2/config/restart

Function: Restarts the gateway
Payload: none
### GET /proxy/rm/v1/…

### POST /proxy/rm/v1/…

Function: Proxy calls to resource manager of Hadoop.  Only works for GET
and POST calls.
### GET /proxy/stram/v2/...

### POST /proxy/stram/v2/…

### PUT /proxy/stram/v2/…

### DELETE /proxy/stram/v2/…

Function: Proxy calls to Stram Web Services.
### POST /ws/v2/applications/{appid}/loggers

Function: Set the logger levels of packages/classes.
Payload:

```json
{
"loggers" : [
{
                "logLevel": value,
                "target": value
        }, ...]
}
```

### GET /ws/v2/applications/{appid}/loggers

Function: Gets the logger levels of packages/classes.
Return:

```json
{
"loggers" : [
{
                "logLevel": value,
                "target": value
        }, ...]
}
```

### GET /ws/v2/applications/{appid}/loggers/search?pattern="{pattern}"

Function: searches for all classes that match the pattern.
Return:

```json
{
         "loggers" : [
                {
"name" : "{fully qualified class name}",
                        "level": "{logger level}"
}
```, ...

]
}
### GET /ws/v2/appPackages

Since: 1.0.4
Function: Gets the list of appPackages the user can view in the system

```json
{
    "appPackages": [
        {
                 "appPackageName": "{appPackageName}",
                 "appPackageVersion": "{appPackageVersion}",
            "modificationTime": "{modificationTime}",
            "owner": "{owner}",
        }, ...
    ]
}
```

### POST /ws/v2/appPackages?merge={replace|fail|ours|theirs}

Since: 1.0.4        
Function: Uploads an appPackage file, merge with existing app package if
exists. Default is replace.
Payload: the raw zip file
Return: The information of the app package
### GET /ws/v2/appPackages/{owner}/{name}

Since: 1.0.4
Function: Gets the list of versions of appPackages with the given name
in the system owned by the specified user

```json
{
    "versions": [
        "1.0-SNAPSHOT"
    ]
}
```

### DELETE /ws/v2/appPackages/{owner}/{packageName}/{packageVersion}

Since: 1.0.4
Function: Deletes the appPackage
### GET /ws/v2/appPackages/{owner}/{packageName}/{packageVersion}/download

Since: 1.0.4
Function: Downloads the appPackage zip file
### GET /ws/v2/appPackages/{owner}/{packageName}/{packageVersion}

Since: 1.0.4
Function: Gets the meta information of the app package
Returns:

```json
{
"appPackageName": "{appPackageName}",
        "appPackageVersion": "{appPackageVersion}",
        "modificationTime":  "{modificationTime}",
        "owner": "{owner}",
        ...
}
```

### GET /ws/v2/appPackages/{owner}/{packageName}/{packageVersion}/configs

Since: 1.0.4
Function: Gets the list of configurations of the app package
Returns:

```json
{
    "configs": [
        "my-app-conf1.xml"
    ]
}
```

### GET /ws/v2/appPackages/{owner}/{packageName}/{packageVersion}/configs/{configName}

Since: 1.0.4
Function: Gets the properties XML of the specified config
Returns:
\<configuration\>
        \<property\>
                \<name\>...\</name\>
                \<value\>...\</value\>
        \</property\>
        …
\</configuration\>
### PUT /ws/v2/appPackages/{owner}/{packageName}/{packageVersion}/configs/{configName}

Since: 1.0.4
Function: Creates or replaces the specified config with the property
parameters specified payload
Payload: configuration in XML
### DELETE /ws/v2/appPackages/{owner}/{packageName}/{packageVersion}/configs/{configName}

Since: 1.0.4
Function: Deletes the specified config
### GET /ws/v2/appPackages/{owner}/{packageName}/{packageVersion}/applications

Since: 1.0.4
Function: Gets the list of applications in the appPackage
Returns:

```json
{
        "applications": [
                {
"dag": {dag in json format},
"file": "{fileName}",
"name": "{name}",
        "type": "{type}",
        "error": "{error}",
        "fileContent": {originalFileContentForJSONTypeApp}
}
```, ...

        ]
}
### GET /ws/v2/appPackages/{owner}/{packageName}/{packageVersion}/applications/{appName}

Since: 1.0.4
Function: Gets the meta data for that application
Returns:

```json
{
"file": "{fileName}",
"name": "{name}",
        "type": "{json/class/properties}",
        "error": "{error}"
"dag": {
        "operators": [
          {
            "name": "{name}",
            "attributes":  {
                "{attributeKey}": "{attributeValue}", ...
            },
            "class": "{class}",
            "ports": [
                  {
                    "name": "{name}",
                    "attributes":  {
                       "{attributeKey}": "{attributeValue}", ...
                     },
                  }, ...
            ],
            "properties": {
               "{propertyName}": "{propertyValue}"
            }
         }, ...
        ],
        "streams": [
          {
            "name": "{name}",
            "locality": "{locality}",
            "sinks": [
                {
                    "operatorName": "{operatorName}",
                    "portName": "{portName}"
                }, ...
            ],
            "source": {
                "operatorName": "{operatorName}",
                "portName": "{portName}"
            }
          }, ...
        ]
}
```,

"fileContent": {originalFileContentForJSONTypeApp}
}
### POST /ws/v2/appPackages/{user}/{appPackageName}/{appPackageVersion}/merge

Function: Merge the configuration, json apps, and resources files from
the version specified from the payload to the specified app package in
the url, without overwriting any existing file in the specified app
package
Payload:

```json
{
 "version": "{versionToMergeFrom}"
}
```

### POST /ws/v2/appPackages/{owner}/{packageName}/{packageVersion}/applications/{appName}/launch[?config={configName}&originalAppId={originalAppId}&queue={queueName}]

Since: 1.0.4
Function: Launches the application with the given configuration
specified in the POST payload
Payload:

```json
{
    "{propertyName}" : "{propertyValue}", ...
}
```

Return:

```json
{
    "appId": "{appId}"
}
```

### GET /ws/v2/appPackages/{owner}/{packageName}/{packageVersion}/operators/{classname}

Since: 1.0.4
Function: Get the properties of the operator given the classname in the
jar

```json
{  
    "properties": [  
        {
          "name":"{className}",
          "canGet": {canGet},
          "canSet": {canSet},
          "type":"{type}",
          "description":"{description}",
          "properties": ...
        },
       …
     ]
}
```

### PUT /ws/v2/appPackages/{owner}/{packageName}/{packageVersion}/applications/{applicationName}[?errorIfExists={true/false}]

Function: Creates or Replaces an application using json. Note that
"ports" are only needed if you need to specify port attributes.  If
errorIfExists is true, it returns an error if the application with the
same name already exists in the app package
Payload:

```json
{
        "displayName": "{displayName}",
        "description": "{description}",
        "operators": [
          {
            "name": "{name}",
            "attributes":  {
                "{attributeKey}": "{attributeValue}", ...
            },
            "class": "{class}",
            "ports": [
                  {
                    "name": "{name}",
                    "attributes":  {
                       "{attributeKey}": "{attributeValue}", ...
                     },
                  }, ...
            ],
            "properties": {
               "{propertyName}": "{propertyValue}"
            }
          }, ...
        ],
        "streams": [
          {
            "name": "{name}",
            "locality": "{locality}",
            "sinks": [
                {
                    "operatorName": "{operatorName}",
                    "portName": "{portName}"
                }, ...
            ],
            "source": {
                "operatorName": "{operatorName}",
                "portName": "{portName}"
            }
          }, ...
        ]
}
```

Return:

```json
{
        "error": "{error}"
}
```

Notes:
Available port attributes to set: AUTO\_RECORD | IS\_OUTPUT\_UNIFIED |
PARTITION\_PARALLEL | QUEUE\_CAPACITY | SPIN\_MILLIS | STREAM\_CODEC |
UNIFIER\_LIMIT
Available locality options to set: THREAD\_LOCAL | CONTAINER\_LOCAL |
NODE\_LOCAL | RACK\_LOCAL?
### DELETE /ws/v2/appPackages/{owner}/{packageName}/{packageVersion}/applications/{applicationName}

Since: 1.0.5
Function: Deletes non-jar based application in the app package
### GET /ws/v2/appPackages/{owner}/{packageName}/{packageVersion}/operators

Since: 1.0.5
Function: Get the classes of operators from specified app package.
Return:

```json
{  
    "operatorClasses": [  
        {
           "name":"{fullyQualifiedClassName}", 
                "title": "{title}",
           "shortDesc": "{description}",
           "longDesc": "{description}",
           "category": "{categoryName}",
                "doclink": "{doc url}",
                "tags": [ "{tag}", "{tag}", … ],
"inputPorts": [
    {
        "name": "{portName}",
        "type": "{tupleType}",
        "optional": true/false  
    }, …
]
"outputPorts": [
    {
        "name": "{portName}",
        "type": "{tupleType}",
        "optional": true/false  
    }, …
],
           "properties": [  
              {
                 "name":"{propertyName}",
                 "canGet": {canGet},
                 "canSet": {canSet},
                 "type":"{type}",
                 "description":"{description}",
                 "properties": ...
              },
              …
           ]
"defaultValue": {
  "{propertyName}": [VALUE], // type depends on property
  // ...
}
```

        }, …
     ]
}
### GET /ws/v2/appPackages/import

Function: List the importable app packages on Gateway's local file
system
Return:

```json
{
        "appPackages: [
                {
"file": "{file}",
"name": "{name}",
"displayName": "{displayName}",
"version": "{version}",
"description": "{description}"
}
```, ...

        ]
}
### POST /ws/v2/appPackages/import

Function: Import app package from Gateway's local file system
Payload:

```json
{
        "files": ["{file}", … ]
}
```

### PUT /ws/v2/systemAlerts/alerts/{name}

Function: Creates or replaces the specified system alert. The condition
has access to an object in its scope called \_topic. An example alert
might take the form of the following:
\_topic["applications.application\_1400294100000\_0001"].allocatedContainers
\> 5
Payload:

```json
{
        "condition":"{condition in javascript}",
        "email":"{email}",
        "timeThresholdMillis":"{time}"
}
```

### DELETE /ws/v2/systemAlerts/alerts/{name}

Function: Deletes the specified system alert
### GET /ws/v2/systemAlerts/alerts?inAlert={true/false}

Function: Gets the created alerts
Return:

```json
{
            "alerts": [{
                "name": "{alertName}",
                    "condition":"{condition in javascript}",
                    "email":"{email}",
                    "timeThresholdMillis":"{time}",
                "alertStatus": {
                             "isInAlert":{true/false}
                        "inTime": "{time}",
                        "message": "{message}",
                        "emailSent": {true/false}
                }
            }, …  ]
}
```

### GET /ws/v2/systemAlerts/alerts/{name}

Function: Gets the specified system alert
Return:

```json
{
        "name": "{alertName}",
"condition":"{condition in javascript}",        
"email":"{email}",
        "timeThresholdMillis":"{time}",
        "alertStatus": {
                     "isInAlert":{true/false}
                "inTime": "{time}",
                "message": "{message}",
                "emailSent": {true/false}
        }
}
```

### GET /ws/v2/systemAlerts/history

Function: Gets the history of alerts
Return:

```json
{
     "history": [
               {
                        "name":"{alertName}",
                           "inTime":"{time}",
                        "outTime": "{time}",
                        "message": "{message}",
                        "emailSent": {true/false}
               }, ...
     ]
}
```

### GET /ws/v2/systemAlerts/topicData

Function: Gets the topic data that is used for evaluating alert
condition
Return:

```json
{
     "{topicName}": {json object data}, ...
}
```

### GET /ws/v2/auth/users/{user}

Function: Gets the info of the given user
Return:

```json
{
"userName": "{userName}",
"roles": [ "{role1}", "{role2}" ]
}
```

### POST /ws/v2/auth/users/{user}

Function: Changes password and/or roles of the given user
Return:

```json
{
"userName": "{userName}",
"oldPassword": "{oldPassword}",
"newPassword": "{newPassword}",
"roles": [ "{role1}", "{role2}" ]
}
```

### PUT /ws/v2/auth/users/{user}

Function: Creates new user
Return:

```json
{
"userName": "{userName}",
"password": "{password}",
"roles": [ "{role1}", "{role2}" ]
}
```

### DELETE /ws/v2/auth/users/{user}

Function: Deletes the specified user
### GET /ws/v2/auth/users

Function: Gets the list of users
Return:

```json
{
"users": [ {
   "userName": "{username1}",
   "roles": [ "{role1}", … ],
   "permissions": [ "{permission1}", … ]
}
```, … ]

}
### POST /ws/v2/login

Function: Login
Payload:

```json
{
"userName": "{userName}",
"password": "{password}"
}
```

Return:

```json
{
    "authScheme": "{authScheme}",
    "userName" : "{userName}",
    "roles": [ "{role1}", … ],
    "permissions": [ "{permission1}", … ]
}
```

### POST /ws/v2/logout

Function: Log out the current user
Return:

```json
{
}
```


# Publisher-Subscriber WebSocket Protocol

## Input

### Publishing


```json
{"type":"publish", "topic":"{topic}", "data":{data}}
```

### Subscribing

```json
{"type":"subscribe", "topic":"{topic}"}
```
### Unsubscribing

```json
{"type":"unsubscribe", "topic":"{topic}"}
```
### Subscribing to the number of subscribers of a topic

```json
{"type":"subscribeNumSubscribers", "topic":"{topic}"}
```
### Subscribing to the number of subscribers of a topic

```json
{"type":"unsubscribeNumSubscribers", "topic":"{topic}"}
```
## Output
### Normal Published Data

```json
{"type":"data", "topic":"{topic}", "data":{data}}
```
### Number of Subscribers:

```json
{"type":"data", "topic":"{topic}.numSubscribers", "data":{data}}
```

# Auto publish topics

data that gets published every one second:

### "applications"

list of streaming applications running in the cluster

### "applications.[appid]"

information about a particular application

### "applications.[appid].containers"

information about containers of a particular application

### "applications.[appid].physicalOperators"

information about operators of a particular application

### "applications.[appid].logicalOperators"

information about logical operators of a particular application

### "applications.[appid].events"

events from the AM of a particularapplication
data that gets published every five seconds:

### "cluster.metrics"

metrics of cluster
