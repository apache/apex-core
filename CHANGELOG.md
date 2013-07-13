# DataTorrent Streaming Platform Release Notes

## Version .3.2

### Bug
* [SPOI-29] - Pig: A inner join node
* [SPOI-55] - A library module/adapter or node for creating keys from general text
* [SPOI-325] - need better error reporting, not sure what host it is trying to connect to
* [SPOI-412] - UI update rate should be 1 sec (default) and allow customization
* [SPOI-441] - Investigate javadoc errors in build
* [SPOI-452] - recompile embedded zmq with 1.6
* [SPOI-482] - Fix the NPM issue when installing UI in the build process
* [SPOI-490] - Clear chart when switching between operators
* [SPOI-497] - Application dashboard starts flickering if I change the column widths too often
* [SPOI-498] - Update rate on UI - Default and Customize


### Improvement
* [SPOI-326] - Byte code obfuscation for future releases: Allow Malhar platform with premium features to be shared with early customers
* [SPOI-447] - Ability to filter app list in the CLI
* [SPOI-451] - Convert daemon stop/start into single service script
* [SPOI-484] - Add server timestamp to containers info

### New Feature
* [SPOI-69] - Persistence Node: A node to persist/spool every window into storage
* [SPOI-89] - Define and implement a RSS input adapter node
* [SPOI-112] - Versioning: Protocol between StramChild to app master
* [SPOI-121] - Parent jira for supporting Pig programming language
* [SPOI-153] - Heartbeat message
* [SPOI-201] - Parent jira for Checkpointing support in streaming platform
* [SPOI-254] - Design a general purpose read from stream and write to hbase node
* [SPOI-259] - Create db adapters - Need one jira for each db
* [SPOI-330] - Add a sql operator
* [SPOI-332] - Do y! finance demo: calculate last price, volume, time, charts, and moving averages
* [SPOI-333] - Do a pi demo
* [SPOI-334] - Write an RSS read operator
* [SPOI-335] - Add persistance/recording for operator, tuples should be stored in-order
* [SPOI-336] - Add persistance/recording for an port of an operator, tuples should be stored in-order
* [SPOI-343] - Design Malhar Deamon for the UI
* [SPOI-344] - Design for Live as well as Historical data access via daemon/stram
* [SPOI-345] - New optimal buffer server to get around memory issues with netty
* [SPOI-347] - Develop cli commands as a foundational part of an operating system
* [SPOI-348] - Allow commands to run as a script through CLI
* [SPOI-356] - Input adapter for DRUID
* [SPOI-357] - Output Adapter for DRUID
* [SPOI-362] - Create Alert operator that does moving average and alerts if that drops by more than X%
* [SPOI-365] - Output Adapter for Redis
* [SPOI-374] - Size limits on log files (STRAM mainly)
* [SPOI-380] - Add security to STRAM
* [SPOI-383] - Implement do-As
* [SPOI-386] - Design latency computations
* [SPOI-387] - Design CPU, Memory, and Network IO usage
* [SPOI-388] - Added resource usage to stats and access via webservice
* [SPOI-391] - Design and implement at least once
* [SPOI-394] - Add input adapter for Redis
* [SPOI-395] - Create an output adapter for Redis
* [SPOI-398] - Test download on Amazon
* [SPOI-405] - Design, document, and test download to work with Apache Hadoop
* [SPOI-416] - Local only Download licensing
* [SPOI-426] - Create an attribute that forces the checkpoint to align with application window boundary
* [SPOI-479] - Design and implement ad data charting
* [SPOI-481] - UI design for application platform charting (live and historical dimensional data)
* [SPOI-485] - Placeholder for list of possible UI work for Summit
* [SPOI-486] - Enable security in the UI (need to check that the user has permission)
* [SPOI-487] - Design and implement new SVG charts
* [SPOI-489] - Alerts
* [SPOI-491] - Create login error pages for UI on secure cluster
* [SPOI-492] - Design UI for resources and latency data
* [SPOI-493] - Design and implement per operator view
* [SPOI-494] - Design and develop the "Operations" dashboard
* [SPOI-495] - The recording tab should show list of recordings by operator or by operator:port
* [SPOI-496] - Securitiy UI (sign-in, get token from deamon, etc.)


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
