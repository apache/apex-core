# DataTorrent Streaming Platform Release Notes

## Version .3.3

### Bug
* [SPOI-418] - Duplicate - Demos need better documentation
* [SPOI-438] - Remove outdated zookeeper dependency from contrib test scope
* [SPOI-455] - Demos cannot be launched after install
* [SPOI-456] - CLI: kill-container should accept container number
* [SPOI-464] - Update README to clarify development environment sydtem requirements
* [SPOI-472] - dashboard fails with empty app list
* [SPOI-478] - Recreate a page view on URL change
* [SPOI-509] - Fix UI build on Linux
* [SPOI-517] - Ensure inputPorts and outputPorts are not undefined before merging them
* [SPOI-519] - Investigate Intermittent cease of publishing from Daemon
* [SPOI-527] - divison by zero exception in StreamingContainerManager for calculating throughput
* [SPOI-554] - Fix MergeSort operator and test 


### Improvement
* [SPOI-434] - Ability to subscribe to buffer server stream at next begin window
* [SPOI-454] - CLI: Feedback when not connected to app
* [SPOI-465] - Increase unit test coverage to at least 50%
* [SPOI-467] - Backbone.js MVC: View -> Model -> Datasource
* [SPOI-469] - Normalize naming convention for all modules
* [SPOI-470] - Provide allocated/planned container count
* [SPOI-500] - Move require calls to widget classes to the top of each page file
* [SPOI-510] - Change API for Datasource module
* [SPOI-512] - Remove dataSource from model attributes
* [SPOI-528] - PortInfoWidget Unit Test
* [SPOI-547] - Convert Notifier module to an Object
* [SPOI-563] - Move WebSocket creation out of DataSource constructor
* [SPOI-582] - Document Front-End Architecture (UML Diagrams)

### New Feature
* [SPOI-91] - Design and implement a socket adapter operator
* [SPOI-135] - Webservice: Provide statistics per streaming operator
* [SPOI-397] - Download Parent Jira
* [SPOI-402] - Document demo examples
* [SPOI-466] - Design and Implement Port View
* [SPOI-468] - Design and Implement Container View
* [SPOI-476] - DT phone home
* [SPOI-503] - Create a pipeline for handling UI feedback
* [SPOI-518] - Implement at most once
* [SPOI-551] - Create Video for Recording Tuples



## Version .2


### Bug
* [SPOI-187] - Evaluate tableausoftware.com
* [SPOI-282] - Create MongoDB read adapter

### Improvement
* [SPOI-204] - Have a UberStram (Inherit from UberAM in Yarn)

### New Feature
* [SPOI-65] - Provide an ability to replay/re-run messages/tuples in the windows not yet purged by streaming ap master
* [SPOI-67] - A buffer server framework
* [SPOI-68] - User parameter for "do not create a sibling node"
* [SPOI-72] - Persisted filenames should be derived via a spec that users can specify
* [SPOI-78] - Parameter for usage of mcast
* [SPOI-81] - Parameter to notify a node as pass through
* [SPOI-86] - Implement an input stream (instream)
* [SPOI-97] - Parameter to tag a stream as "need to be on the bus"
* [SPOI-103] - Parameter(s) to define spike trigger for alert
* [SPOI-105] - Spike Processing: Load balancing - split node into parallel nodes
* [SPOI-108] - HA - Spike Processing: Propagate stream hold back upstream
* [SPOI-113] - Versioning: Protocol between hadoop containers and app master
* [SPOI-120] - Pig: Distinct Operation Node
* [SPOI-123] - CLI: Design command line parameters
* [SPOI-124] - CLI: Access to current state of DAG
* [SPOI-125] - CLI: Access to window stats
* [SPOI-126] - CLI: Access to node performance and internals
* [SPOI-130] - Ensure that CLI and Webservice use the same code
* [SPOI-131] - Webservice: To get the current DAG
* [SPOI-132] - Webservice: Design RESTful webservice for the streaming platform
* [SPOI-141] - Webservice: To list uptime
* [SPOI-142] - Webservice: Total message throughput and lost message
* [SPOI-148] - A dashboard node
* [SPOI-149] - Outstream Optimization: Add ability to select stream-out of tuples in batches
* [SPOI-166] - Inherit authentication schemes from Hadoop
* [SPOI-169] - Node to authenticate before providing access
* [SPOI-180] - Protocol for public service registry on stram channel
* [SPOI-182] - Parent jira for UI Tool/Dashboard
* [SPOI-183] - Dashboard should display all streaming app currently running
* [SPOI-185] - Show live app data
* [SPOI-208] - Design and Implement sticky partitioning for load balancing
* [SPOI-213] - Inline stream: Users should be able to specify inline stream
* [SPOI-214] - Streams with persistence cannot be inlined
* [SPOI-220] - Allow users to statically assign a partition policy
* [SPOI-222] - Runtime skew management option
* [SPOI-242] - Parent jira for revenue generating products on streaming platform
* [SPOI-249] - Parent jira for supporting ProtoBufs on all parts of streaming platform
* [SPOI-252] - Design and Implement an input hdfs file adapter node
* [SPOI-257] - Allow client side custom validation
* [SPOI-279] - Add a validateSchema() call on each module
* [SPOI-283] - Create MongoDB write module
* [SPOI-284] - Create MySql read adapter module
* [SPOI-285] - Create MySql write module
* [SPOI-287] - A GroupBy key module (also for Pig)


## Version .1

### Bug
* [SPOI-24] - Define and implement filter node
* [SPOI-25] - Compare operator nodes
* [SPOI-26] - Aggregate operator nodes
* [SPOI-27] - Sort node
* [SPOI-28] - Math operator node
* [SPOI-30] - Fork and Merge streams/nodes
* [SPOI-32] - Provide basic tuple operations: Merge and Filter
* [SPOI-50] - Launch test - Test word count
* [SPOI-51] - General Purpose Read a file and write to bus node
* [SPOI-54] - General purpose hash node (reduce, ref count, last value/out, ...)
* [SPOI-56] - General purpose sort node

### New Feature
* [SPOI-1] - Design message windows
* [SPOI-2] - Design DAG Input Format (hadoop conf format)
* [SPOI-3] - Design Interface with Yarn (Application Master) -> Streaming Application Master
* [SPOI-4] - Design dnode processing
* [SPOI-82] - Parameter to notify a node a "emit of window close" or stateful
* [SPOI-84] - Window Generator Logging
* [SPOI-85] - StramChild logging
* [SPOI-87] - Implement an output stream (outstream)
* [SPOI-88] - Define and implement "time" field
* [SPOI-92] - Design and implement a http protocol adapter node
* [SPOI-94] - Optimize: Implement merging two streaming nodes
* [SPOI-98] - Design communication interface between hadoop containers and streaming application master
* [SPOI-100] - Design end of window message
* [SPOI-109] - General purpose buffer node
* [SPOI-111] - Versioning: Tuples schema
* [SPOI-116] - Pig: A filter by Operation node
* [SPOI-119] - Sampling Operation Node:
* [SPOI-144] - Design and implement StreamContext
* [SPOI-145] - Design and implement a streamConfig
* [SPOI-146] - Parent jira for stream submission to Yarn
* [SPOI-150] - Sort node without window boundary
* [SPOI-151] - Parent jira for message types
* [SPOI-154] - Design and implement start of window message
* [SPOI-155] - Parent jira for design and implementation of commit (at end of window)
* [SPOI-209] - Design and implement how stram would initialize a dag onto hadoop containers
* [SPOI-210] - A general purpose map node
* [SPOI-247] - Evaluate sending a control tuple(s) from the first level node in the dag
* [SPOI-253] - Design a general purpose read from stream and write to hdfs node
* [SPOI-262] - Evaluate Flumebase as it has high level streaming constructs
* [SPOI-267] - Evaluate and integrate with LMAX Disruptor as a choice for tuple throughput
* [SPOI-278] - Run streaming application in local mode for debugging, testing, etc.
* [SPOI-288] - Have a unique counter module
* [SPOI-289] - Create an queue node
* [SPOI-290] - Module: Create reverse index module that provides value in an ArrayList
* [SPOI-291] - Create and arithmetic quotient node
* [SPOI-292] - Create an arithmetic margin module
