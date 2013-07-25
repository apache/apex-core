#Release Notes - Streaming Platform

## Version 0.3.3

## Bug
* [SPOI-418] - Duplicate - Demos need better documentation
* [SPOI-438] - Remove outdated zookeeper dependency from contrib test scope
* [SPOI-456] - CLI: kill-container should accept container number
* [SPOI-464] - Update README to clarify development environment sydtem requirements
* [SPOI-472] - dashboard fails with empty app list
* [SPOI-478] - Recreate a page view on URL change
* [SPOI-509] - Fix UI build on Linux
* [SPOI-517] - Ensure inputPorts and outputPorts are not undefined before merging them
* [SPOI-519] - Investigate Intermittent cease of publishing from Daemon
* [SPOI-527] - divison by zero exception in StreamingContainerManager for calculating throughput
* [SPOI-554] - Fix MergeSort operator and test 
* [SPOI-571] - Siteops demo hangs
* [SPOI-604] - Empty site subdirectory in dist installer
* [SPOI-607] - Auto publish websocket data get stuck
* [SPOI-618] - Start and stop recordings broke from DataSource change
* [SPOI-622] - Get Port REST request fails for running applications after 399
* [SPOI-629] - Fix DataSource.stopOpRecording usage (options object).
* [SPOI-653] - Fix name in package.json
* [SPOI-655] - Fix "Cannot read property 'ports' of undefined" bug with RecordingModel
* [SPOI-659] - PortPageView should override the cleanUp method to unsubscribe to port topic
* [SPOI-669] - Daemon starts secondary process if already running
* [SPOI-673] - Operator List Widget only needs appId, not whole app model instance
* [SPOI-733] - Incorrect documentation in README.txt developer version

## Improvement
* [SPOI-454] - CLI: Feedback when not connected to app
* [SPOI-467] - Backbone.js MVC: View -> Model -> Data Source
* [SPOI-469] - Normalize naming convention for all modules
* [SPOI-500] - Move require calls to widget classes to the top of each page file
* [SPOI-510] - Change API for Datasource module
* [SPOI-512] - Remove dataSource from model attributes
* [SPOI-528] - PortInfoWidget Unit Test
* [SPOI-547] - Convert Notifier module to an Object
* [SPOI-563] - Move WebSocket creation out of DataSource constructor
* [SPOI-582] - Document Front-End Architecture (UML Diagrams)
* [SPOI-596] - Move require calls to the top of pages.js
* [SPOI-602] - Get siteops demo to work without needed high bandwidth for UI
* [SPOI-608] - Rename Page Views according to naming conventions
* [SPOI-615] - Add 'port' to breadcrumb label in port view
* [SPOI-616] - The 'type' attribute in portmodel is not being extrapolated in subscribeToUpdates
* [SPOI-621] - The DataSource should URL-Encode port name in the getPort method
* [SPOI-676] - Return better response for apps.list when no apps running

## New Feature
* [SPOI-135] - Webservice: Provide statistics per streaming operator
* [SPOI-397] - Download Parent Jira
* [SPOI-402] - Document demo examples
* [SPOI-460] - Create Preconfigured Sandbox
* [SPOI-466] - Design and Implement Port View
* [SPOI-468] - Design and Implement Container View
* [SPOI-476] - DT phone home first cut
* [SPOI-503] - Create a pipeline for handling UI feedback
* [SPOI-518] - Implement at most once for Operators with one or zero connected input ports.
* [SPOI-598] - Daemon to serve historical stats data
* [SPOI-700] - Remove demo/groupby

## Sub-task
* [SPOI-453] - Update the mobile demo to version with map
* [SPOI-513] - Move JavaSerializationStreamCodec operator to library/util package
* [SPOI-515] - GitHub Release: Review/format source and docs for library/io/AxctiveMQ Input Operator.
* [SPOI-516] - GitHub repository structure and build system changes
* [SPOI-520] - Test strategy for operators that currently use LocalMode
* [SPOI-521] - Review/fix code formatting / style issues
* [SPOI-524] - Refactor script operators
* [SPOI-526] - Modify library to use CollectorTestSink
* [SPOI-539] - Move PerformanceTestCategory annotation to library
* [SPOI-541] - Port View Unit Tests
* [SPOI-542] - Publish library tests jar
* [SPOI-543] - Remove dependency to Tuple for library tests
* [SPOI-545] - DataSource Unit Tests
* [SPOI-546] - Determine best approach to mocking require()d modules
* [SPOI-552] - RedisOutputOperator stops running abruptly
* [SPOI-553] - Test code for InnerJoin/InnerJoin2 operators
* [SPOI-555] - Fix Unifier in match/change operators
* [SPOI-556] - Graphs show up after a delay with the new ads demo web changes.
* [SPOI-558] - Benchmark tests depend on STRAM
* [SPOI-559] - Move benchmark tests to a separate module
* [SPOI-560] - GitHub release: SumTest math operator still has reference to STRAM
* [SPOI-561] - Duplicate - Separate benchmark tests into a separate module
* [SPOI-562] - Fix HdfsOutputTest to not depend on STRAM
* [SPOI-565] - GitHub release: Change Http operator tests to use mortbay jetty
* [SPOI-566] - Remove reference to StramTestSupport from KafkaInputOperatorTest
* [SPOI-567] - KafkaInputOperatorTest is using DAG
* [SPOI-568] - Modify contrib build to not depend on STRAM
* [SPOI-583] - Implement a sample PubSubWebSocket servlet for testing
* [SPOI-584] - Contrib test classes have dependency to bufferserver
* [SPOI-585] - Implement a helper OperatorContext
* [SPOI-586] - PageLoaderView Unit Tests
* [SPOI-587] - Create SQL operator  base interface/class
* [SPOI-588] - Add SQL Select Oprator  to library
* [SPOI-589] - Create Sql Update operator in library
* [SPOI-590] - Create Sql Delete operator in library
* [SPOI-591] - Create SQL GourpBy/OrderBy Operator in library
* [SPOI-593] - Create SQL Outer join operator in library
* [SPOI-605] - Port View - Info and Overview Widgets
* [SPOI-636] - Apache Open Source Release : Review cods and source code for lirary/algo operator.
* [SPOI-637] - Create output Unifier on library/algo/BottomNOperator.
* [SPOI-639] - Create output port unifier for library/algo/Distinct Operator.
* [SPOI-640] - Create output port unifier for library/algo/FirstMatchMap Operator.
* [SPOI-642] - Create output port unifier for library/algo/FirstMatchStringMap Operator.
* [SPOI-644] - Create output port unifier for library/algo/FirstN Operator.
* [SPOI-645] - HttpOutputOperatorTest is failing
* [SPOI-646] - Create output port unifier for library/algo/InsertSort Operator.
* [SPOI-647] - NavModel/Router Unit Tests
* [SPOI-648] - Remove hash map output port from Insert sort operator.
* [SPOI-649] - Create output port unifier for library/algo/InvertIndex<K, V> Operator.
* [SPOI-650] - Mocha global leaks issue
* [SPOI-651] - Create output port unifier for library/algo/InvertIndexArray<K, V> Operator.
* [SPOI-652] - SlidingWindowTest is failing
* [SPOI-654] - Fix output port unifer for LeastFrequentKey operator.
* [SPOI-658] - Add Mocha Console Reporter
* [SPOI-661] - PageLoaderView Router Navigation Unit Tests
* [SPOI-662] - Refactor Bash Script oPerator.
* [SPOI-663] - Refactor Python script operator.
* [SPOI-664] - Fix output port unifier for MostFrequentKey Operator
* [SPOI-665] - Fix output port unifier for MostFrequentKeyValMap Operator
* [SPOI-666] - Fix output port unifier for libarry/algo/TopN Operator
* [SPOI-667] - Change output port for library/algo/TopNUnique Operator, add output port unifier.
* [SPOI-668] - Remove demo operator TupleOperator from library.
* [SPOI-671] - Create unifier on output port for library/algo/UniqueKeyValOperator
* [SPOI-672] - Remove demo operator : libarry/algo/WindowHolder
* [SPOI-675] - Review source/docs for library/io operators
* [SPOI-684] - Remove stram dependency from hdfs input operator test.
* [SPOI-686] - Fix HTTP output operator and test, failing right now.
* [SPOI-688] - Remove TestTupleCollector from io tests, it is not a test for any thing.
* [SPOI-689] - Remove com.library.io.anootation empty test package.
* [SPOI-690] - Move com.library.io.helper  test to sample library.
* [SPOI-691] - Review source code/docs for library/logs operator.

## Technical task
* [SPOI-557] - Review/change code for MergeSort Operator

## Task
* [SPOI-205] - Protocol for stram to change network/message bus parameters of the outstream of output adapter dnode
* [SPOI-324] - Performance data to be written to HDFS
* [SPOI-459] - Test installation on OS X
* [SPOI-473] - GitHub Release : Code review for library/math operators
* [SPOI-474] - Document mobile demo
* [SPOI-475] - Document twitter demo
* [SPOI-477] - Review/format doc for apche open source release of library operators.
* [SPOI-499] - Review/format doc for apche open source release of library stream operators.
* [SPOI-504] - GitHub Release : Review cods and source code for lirary/multi window operator.
* [SPOI-505] - GitHub Release: Review/Format source/docs/test for AbstractSlidignWindowKeyVal Operator
* [SPOI-506] - GitHub Release : Review/Format source/docs/test for library/MultiSlidingWindowKeyVal Operator
* [SPOI-507] - GitHub Migration: Review/Format source/docs/test for library/SimpleMovingAverage Operator
* [SPOI-508] - GitHub Release : Review/Format source/docs/test for library/MultiSlidingWindowRangeKeyVal Operator
* [SPOI-511] - GitHub Release : Review/Format source/docs/test for library/logs operator
* [SPOI-514] - Open Source Transition for Library/Contrib/Demos
* [SPOI-530] - Operator tests dependent on stram.
* [SPOI-532] - Remove stram dependency from com.datatorrent.lib.testbench.EventGeneratorTest
* [SPOI-533] - Remove stram dependency from com.datatorrent.lib.testbench.RandomEventGeneratorTest
* [SPOI-534] - Remove stram depnedncy from com.datatorrent.lib.testbench.SeedEventGeneratorTest
* [SPOI-535] - GitHub release - Remove stram dependency from com.datatorrent.lib.math.MaxKeyValTest
* [SPOI-536] - Remove stram dependency from com.datatorrent.lib.math.MinKeyValTest
* [SPOI-537] - Remove stram dependency from com.datatorrent.lib.math.MaxMapTest
* [SPOI-540] - Remove stram dependency from com.datatorrent.lib.io.ActiveMQInputOperatorTest
* [SPOI-549] - Create a process for change.log for .3.3 release
* [SPOI-550] - Need license file changes to add DT Phone Home
* [SPOI-564] - Duplicate - Need to update license text to reflect "DT Phone Home"
* [SPOI-572] - Create a library for sql operators
* [SPOI-573] - Move GroupBy to Sql library
* [SPOI-574] - Move innerjoin operator to Sql lib
* [SPOI-575] - Move util/DerbySqlStreamOperator to sql libraray
* [SPOI-576] - Move util/AbstractSqlStreamOperator to sql library
* [SPOI-577] - Move OrderByKey operator to sql library
* [SPOI-578] - move algo/OrderbyKeyDesc operator to sql lib
* [SPOI-579] - move algo/orderbyvalue to sql library
* [SPOI-580] - move algo/orderbyvaluedesc to sql lib
* [SPOI-581] - move algo/innerjoincondition to sql lib
* [SPOI-592] - Create SQL Inner Join Operator in library
* [SPOI-594] - Archive release builds
* [SPOI-601] - Include twitter demo on demo server
* [SPOI-623] - Formalize a release procedure for future release
* [SPOI-625] - Add footer to guides
* [SPOI-626] - Get site ops demo UI to scale
* [SPOI-627] - Site ops demo UI changes
* [SPOI-635] - Ensure notice in html javadocs
* [SPOI-670] - Create output port unifier in libarry/algo/UniqueCounter Operator
* [SPOI-692] - change UI version to match streaming platform version
* [SPOI-697] - Demo of JIRA commits feature
* [SPOI-702] - Create a google groups for GitHub project Malhar
* [SPOI-705] - Setup a server for customer apps



## Version 0.3.2

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

## Story
* [SPOI-371] - Review Storm features vs our open source plan
* [SPOI-372] - Ensure that premium feature implementation is hard for outsiders to do on the open source tree

## Task
* [SPOI-14] - Dynamic run time optimization framework: Load balancing and load shedding
* [SPOI-39] - Should be able to track window id through the DAG
* [SPOI-40] - Output messages of each node should be persisted/buffered
* [SPOI-63] - Output Adapter logging
* [SPOI-64] - Levels in logging
* [SPOI-231] - Demo tweeter feed analysis on Hadoop 2.0 with basic streaming setup
* [SPOI-273] - Setup CI and run existing demos to ensure that code does not break any
* [SPOI-419] - Certify our build on Amazon
* [SPOI-420] - Convert mobile location demo to a google maps demo
* [SPOI-421] - Ensure that checkpointing happens at Application window boundary
* [SPOI-422] - Allow an operator developer to enable checkpointing within an application window
* [SPOI-423] - Have a "ALLOW_CHECKPOINT_WITHIN_WINDOW" attribute (default is FALSE)
* [SPOI-424] - Document new checkpointing logic in app dev guide and op dev guide, add attribute to operations guide
* [SPOI-442] - ASF License header for downloadable sources
* [SPOI-443] - Packaging for dev and cluster versions
* [SPOI-480] - Add aggregate i/o buffer server bytes for application level
* [SPOI-483] - Tie up loose ends for summit version
* [SPOI-488] - Remove failure count column from "Live" table

