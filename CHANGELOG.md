#Release Notes - Streaming Platform

## Version 0.3.4

## Bug
* [SPOI-569] - Ads demo charting is not stable; Needs cleanup
* [SPOI-570] - Mobile demo does not run forever
* [SPOI-599] - Sometimes in the metrics charts widget some charts are flat
* [SPOI-617] - Change error code from 500 to 400 for bad request
* [SPOI-630] - Stop recording action does not work
* [SPOI-660] - Recording shows a red light, but no recording is being done
* [SPOI-674] - AppModel and AppInstanceView need clean-up
* [SPOI-704] - Red dots show up in the dashboard, as if the operator is recording
* [SPOI-714] - Ads UI load is alow
* [SPOI-715] - Ads demo chart is 25 mins behind
* [SPOI-724] - Fix LocalFileInputOperator logic and documentation
* [SPOI-730] - Developer version for Mac OS X
* [SPOI-731] - Cannot record anymore. Tuple recorder is broken
* [SPOI-732] - Cannot stop recording
* [SPOI-742] - Installation/Compilation fails with maven-eclipse plugin
* [SPOI-744] - Facilitate the "accepted" app state for the app instance view
* [SPOI-747] - StreamingContainerManager.getContainerAgents() returns inactive containers
* [SPOI-748] - Build Kestrel as 3rd party library and depend on it instead of including the code with operator library
* [SPOI-751] - browser CPU utilization excessive with larger number of updates
* [SPOI-755] - Evaluate ads dimensional data demo
* [SPOI-756] - Account for single-item, non-list responses from web services
* [SPOI-757] - docs directory re-link for new version install
* [SPOI-762] - Application Model not updating correctly, causing graph issues
* [SPOI-764] - Malhar engine build is including StramAppMaster in engine jar in some cases
* [SPOI-765] - Datasource constructing tuple GET URL incorrectly for tupleviewer
* [SPOI-768] - Log file rotation support issue
* [SPOI-804] - If a node is bad the app master should choose a different node for the containers.
* [SPOI-806] - list-apps shows duplicate entries
* [SPOI-807] - downgrade jvm on node3
* [SPOI-809] - Add pig distinct semantic operator to library.
* [SPOI-811] - shutdown app does not work when not connecting to an app
* [SPOI-842] - Generated javadocs are missing several packages
* [SPOI-845] - Installer test fails when executing from different directory
* [SPOI-849] - Start recording from port list
* [SPOI-863] - Display application-wide latency in Application View
* [SPOI-883] - StramDelegationTokenManager should not be started if security is not enabled
* [SPOI-888] - Account for delay in Stram initialization in recently launched apps
* [SPOI-890] - Stram unit test is creating events data in stram/stram
* [SPOI-892] - getAppInfo throws an exception during the beginning of the application
* [SPOI-902] - Tupleviewer filter by port fails
* [SPOI-903] - Tupleviewer preview of loaded tuples not rendering
* [SPOI-905] - Review/fix license headers in open source
* [SPOI-911] - Sync recording call fails with 500 error
* [SPOI-915] - Log collection tool for hadoop cluster
* [SPOI-918] - Partitioning stopped on MobileLocatorDemo instance 48
* [SPOI-933] - Specify application names in stram configuration file for the customer demos on the cluster.
* [SPOI-946] - Certify Sandbox for Ubuntu
* [SPOI-951] - Auto completed file name does not work with launch command
* [SPOI-952] - Remove operator returns error about input stream still connected.
* [SPOI-958] - LocalFsInputOperator test is failing
* [SPOI-965] - UI: Make sure empty lists and single-item lists in JSON returned from Daemon is handled properly
* [SPOI-969] - UI: Stop recording fails with error saying some recording name was not found with operator undefined.
* [SPOI-971] - Aggregation function for application stats fails intermittently 
* [SPOI-974] - PageLoaderView Unit Test
* [SPOI-981] - Ensure "ports" are in the operators of the logicalPlan response
* [SPOI-991] - Daemon: windowIds must be sent as strings
* [SPOI-997] - Compute throughput as rolling average


## Improvement
* [SPOI-434] - Ability to subscribe to buffer server stream at next begin window
* [SPOI-450] - CLI: Allow wildcard in launch file name
* [SPOI-463] - The appMetrics widget should remember the choices on the graphs (on/off)
* [SPOI-470] - Provide allocated/planned container count
* [SPOI-614] - Better Error Handling/Error Pages
* [SPOI-620] - Allow recording from operator/port page
* [SPOI-624] - Normalize appid/appId opid/operatorId and similar discrepancies
* [SPOI-677] - Setup both Core and Front-End to run on local environment
* [SPOI-685] - Explore other charting libraries to replace custom chart module
* [SPOI-687] - Improve CSS for tupleviewer
* [SPOI-696] - Only update whole application collection on applist page
* [SPOI-712] - Explicit Errors when daemon provides data in unexpected format
* [SPOI-753] - Overview metrics for Port View should have comma separations
* [SPOI-835] - CLI needs to be able to handle spaces and escape characters
* [SPOI-844] - operatorClass REST call should also accept "chart" or "filter" for charting and alert filters
* [SPOI-901] - Add palette for containers list
* [SPOI-938] - Centralize reused templates for links

## New Feature
* [SPOI-70] - Parent JIra: Webservices for Streaming Application
* [SPOI-73] - Job completed report
* [SPOI-74] - Reporting: Job completion report should have the list of persisted files per node
* [SPOI-75] - Parameter to specify if persisted file names should be included in the job completion report
* [SPOI-127] - CLI: Throughput data (streams)
* [SPOI-129] - CLI: Streaming Ap Master data
* [SPOI-134] - Webservice: Stream data/throughput per stream in the application
* [SPOI-138] - Webservices: Data on committed windows
* [SPOI-139] - Webservice: Latency across the dag/application
* [SPOI-140] - Event Logging
* [SPOI-143] - Webservice: Bottleneck analysis
* [SPOI-219] - Evaluate if we need a naming scheme to identify a physical node of a logical dnode
* [SPOI-337] - Do an in-node stream by user choice
* [SPOI-403] - Design license file format
* [SPOI-417] - Licensing Specification
* [SPOI-460] - Create Preconfigured Sandbox
* [SPOI-471] - Stream View
* [SPOI-551] - Create Video for Recording Tuples
* [SPOI-628] - Web service for event logging
* [SPOI-698] - Add streamquery operators that support expression
* [SPOI-699] - Add pass through streamquery operators
* [SPOI-710] - DatatTorrent Wiki
* [SPOI-718] - Implement at most once for Operators with two or more connected input ports.
* [SPOI-719] - Implement Dag Validations for At-Most-Once
* [SPOI-738] - DT Phone Home Phase II
* [SPOI-829] - Calculate overall latency for application
* [SPOI-830] - Deduce critical path in DAG for application
* [SPOI-833] - operator's latency should be shown in the  UI
* [SPOI-891] - Create a web service API for describing operator class
* [SPOI-906] - Add palette for ports list
* [SPOI-908] - Selecting recordings from recording list fails
* [SPOI-909] - CSS issues with tupleviewer when port name too long
* [SPOI-920] - Application DAG UI Integration
* [SPOI-921] - Sandbox icons
* [SPOI-934] - Alert List Widget - Date/Time
* [SPOI-936] - Alert List Widget - Dynamic Width
* [SPOI-949] - Documentation processor from markdown to html


## Task
* [SPOI-60] - Streaming app master logging
* [SPOI-191] - Enable compression for end of window data or for throttled N tuples (N sent together)
* [SPOI-198] - Evaluate if buffer server should retain data in a compressed state
* [SPOI-199] - Evaluate if we need buffer server to keep data in compressed state
* [SPOI-425] - Checkpointing for exactly once operator recovery
* [SPOI-514] - Open Source Transition for Library/Contrib/Demos
* [SPOI-603] - Evaluation version for Mac OS X
* [SPOI-631] - Make a site operations video
* [SPOI-633] - Need changelog for new version
* [SPOI-656] - Performance metrics charts does not remember preferences
* [SPOI-681] - Added library description to index.html
* [SPOI-682] - Display Application DAG in UI (evaluation)
* [SPOI-706] - Allow app name to be specified in launch time config file
* [SPOI-708] - Set up demo procedures
* [SPOI-713] - Technical evaluation of UIs
* [SPOI-717] - Create launch macros on demo server
* [SPOI-734] - Push 0.3.3 to demo server
* [SPOI-735] - Launch the latest software on customer app server
* [SPOI-736] - Certify customer application server
* [SPOI-737] - Clean up redis history on customer apps
* [SPOI-740] - Updated documentation generation process
* [SPOI-745] - DT Phone Home Server side work
* [SPOI-754] - Create other user for node0 launches
* [SPOI-759] - Technical Evaluation
* [SPOI-760] - Format comments for ASF project for Malhar GitHub
* [SPOI-761] - Format comments in API
* [SPOI-769] - Fix sql DeleteOperator to make it pass thru.
* [SPOI-770] - Fix SelectOperator to make it pass thru.
* [SPOI-771] - Fix sql UpdateOperator code to make it pass thru.
* [SPOI-773] - Add seolect expression index.
* [SPOI-774] - Fix sql InnnerJoin operator join condition.
* [SPOI-775] - Fix sql Outer Join operator to merge left/right/full join sql semantic.
* [SPOI-776] - Add sql Having  semantic operator to library.
* [SPOI-777] - Add sql select top operator semantic to library.
* [SPOI-778] - Add sql select between condition sematic to library.
* [SPOI-779] - Add  sql select  compound condition  AND/OR semanticf to library. 
* [SPOI-780] - Add sql in condition semantic to library
* [SPOI-781] - Add sql like condition semantic to library.
* [SPOI-782] - Add having coprae value semantic to library.
* [SPOI-783] - Add having condition interface to support sql having operator semantic.
* [SPOI-784] - Add sql select unary expression semantic to library.
* [SPOI-785] - Create sql binary expreesion index semantic in library.
* [SPOI-786] - Add sql select negate index semantic to library.
* [SPOI-787] - Add sql slect sum index semantic to library.
* [SPOI-788] - Add sql select string mid index semantic to library.
* [SPOI-789] - Add sql slect string len semantic to library.
* [SPOI-790] - Add sql select string upper/lower semantic to library.
* [SPOI-791] - Add sql round double semantic to library.
* [SPOI-792] - Add sql select rtound double semantic to library.
* [SPOI-794] - Add sql sleect count aggregate semantic to libarry.
* [SPOI-795] - Add sql select min/max fucntion semantic to library.
* [SPOI-796] - Add sql select first/last aggregate semantic to library.
* [SPOI-797] - Add sql sleect sum aggregate semantic to library.
* [SPOI-798] - Add pig group operator semanitc to library.
* [SPOI-799] - Add PIG filter operator semantic to library.
* [SPOI-800] - Add PIG cross operator semantic to library.
* [SPOI-801] - Add PIG split operator semantic to library.
* [SPOI-812] - Design wireframes for widget for SiteStats 
* [SPOI-814] - Prototype UI dashboard change to facilitate "app store" paradigm
* [SPOI-815] - Add pig join(inner) semantic operator to library.
* [SPOI-816] - Add pig join(outer => left/right/full) semantic operator to library.
* [SPOI-820] - Implement Dimension Operator that allows custom explosion
* [SPOI-825] - Evaluate non-Hadoop Streaming Platforms IDE
* [SPOI-826] - Add pig order by operator semantic to library.
* [SPOI-827] - Add pig limit operator semantic to library.
* [SPOI-828] - Add pig stream operator semantic to library.
* [SPOI-836] - Design Alert-related API
* [SPOI-840] - Add spark add flat map function semantic to operator library.
* [SPOI-848] - Introduce ContainerLocal as replacement for Inline
* [SPOI-850] - Evaluate Hadoop IDE
* [SPOI-851] - Evaluate JavaScript  data visualization libraries
* [SPOI-858] - Class loader support issue: org.fusesource.hawtbuf.UTF8Buffer.class 
* [SPOI-859] - Evaluate Streaming Platform Back-End Development Workflow
* [SPOI-860] - Evaluate Node.js as Back-End for Site Stats Demo
* [SPOI-864] - Redesign REST for altered escalation approach
* [SPOI-865] - Create "List of Alerts" widget
* [SPOI-866] - Add DataSource methods for alert REST API
* [SPOI-879] - Create a data list of grid nodes
* [SPOI-881] - Create Compute-Local api
* [SPOI-882] - Add compute local api to documents
* [SPOI-884] - Update dhcp configuration on cluster nodes.
* [SPOI-885] - Create prototype with Node.js + Redis + REST for Site Stats
* [SPOI-886] - Implement remove logical operator
* [SPOI-889] - Alerts persistence with Backbone.js models
* [SPOI-893] - Externalize UI settings
* [SPOI-894] - Implement JavaScript Filter operator for Alerts
* [SPOI-910] - Node.js Mock Server and JSONP Cross-Domain Requests
* [SPOI-913] - Flesh out "kill app" command from the instance view
* [SPOI-914] - Alerts REST API Error Handling
* [SPOI-935] - Make patch for flawed start/stop recording mechanism
* [SPOI-939] - Remove references to unfinished features for 0.3.4 release
* [SPOI-943] - Certify Sandbox on various OS
* [SPOI-944] - Certify Sandbox on Mac
* [SPOI-947] - Certify Sandbox for Windows
* [SPOI-948] - Improvements in sandbox based on OS certification feedback
* [SPOI-957] - Update license header
* [SPOI-970] - Download latest G! documents
* [SPOI-985] - Macro argument expansion in stramcli
* [SPOI-1006] - Update @since tags for 0.3.4

## Sub-task
* [SPOI-880] - Investigate options for sandbox environment
* [SPOI-922] - Sandbox default hadoop configurations
* [SPOI-923] - Sandbox demo script and application launcher
* [SPOI-924] - Sandbox size and performance optimizations
* [SPOI-926] - Sandbox documentation
* [SPOI-942] - Demo launch page
* [SPOI-990] - Add launch-demo macro to clirc during install
* [SPOI-1004] - End user license agreement updates

## GitHub - DataTorrent/Malhar
* [12] - Site Stats Operator and TopNOperator issues
* [10] - getTopN() function returns ArrayList in com.datatorrent.lib.util.TopNSort class
* [9] - Another Bug in offer(E e) function of com.datatorrent.lib.util.TopNSort.java
* [8] - Bug in offer(E e) function of com.datatorrent.lib.util.TopNSort.java
* [7] - Add ability to configure timeout of RedisOutputOperator
* [6] - Add a continue on error functionality to AbstractKeyValueStoreOutputOperator
* [5] - Add rollback to AbstractKeyValueStoreOutputOperator.
* [3] - Map Reduce job tracker 
* [2] - Fix application name of mobile demo
* [1] - Console Operators


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

