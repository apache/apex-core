#Release Notes - Streaming Platform

## Version 0.9.4

## Bug
* [SPOI-455] - Cleanup maven repository workaround in install.sh
* [SPOI-1636] - Update all node1 demo to 0.9.3 release
* [SPOI-1774] - Thread local performance drop from 65M to 40 M tuplesProcessed/sec
* [SPOI-1775] - MROperator demo applications fail when launched from Sandbox
* [SPOI-1847] - Intermittent - WebSocket Publishing ignores endWindow
* [SPOI-1864] - POC partitioned operators not getting correct initial state
* [SPOI-1866] - Datatorrent applications not starting on CDH5 Vagrant cluster
* [SPOI-1867] - Investigate InstallAnywhere use for DataTorrent installation
* [SPOI-1882] - When not able to contact license agent, the application should not die
* [SPOI-1883] - Stram crashes with Unknown-Container issue
* [SPOI-1885] - Determine minimum amount of memory needed to run twitter app
* [SPOI-1886] - Determine minimum amount of memory needed to run mobile app
* [SPOI-1888] - Install license in the new console configuration
* [SPOI-1889] - Licensing needs to support eval and free
* [SPOI-1902] - Dynamic MxN partitioning does not handle scale down to single M instance 
* [SPOI-1903] - MiniClusterTests fails because ~/.dt/dt-site.xml dependency
* [SPOI-1906] - history does not get flushed to the history file until the next command prompt
* [SPOI-1917] - Licensing error
* [SPOI-1922] - Loading license does not work
* [SPOI-1923] - Close button does not work in several pop-up windows
* [SPOI-1936] - No uninstall script available with the installer. 
* [SPOI-1953] - Installation video on our website should be refreshed to reflect the latest version(s)
* [SPOI-1960] - application incorrectly marked as succeeded
* [SPOI-1968] - 404 for logical plan url
* [SPOI-1970] - Negative requested container count logged by Stram
* [SPOI-1976] - Fix AdsDimensions in certification 
* [SPOI-1992] - the latency in the application overview freezes after a container gets killed.
* [SPOI-1999] - Evaluate Yarn cluster issue
* [SPOI-2002] - Gateway fails to load properties
* [SPOI-2037] - Redirect to welcome page on first run / install
* [SPOI-2039] - DTGateway logs to .dt/logs in service mode
* [SPOI-2040] - Apache logs application under contrib folder fails to run
* [SPOI-2047] - dtgateway service not starting when machine boots
* [SPOI-2049] - Better error message on invalid hadoop directory
* [SPOI-2051] - RPM install reports install script failure
* [SPOI-2052] - DTGateway restart fails when running with -service
* [SPOI-2055] - dtgateway service reports OK on startup failure
* [SPOI-2056] - LicensingAgentProtocolHelper.getLicensingAgentProtocol gets stuck when YARN is not running
* [SPOI-2057] - DTCLI is not working after intallation
* [SPOI-2058] - launch-demos macro not available after installing the platform from self extracting intaller
* [SPOI-2059] - Show understandable error message if the root hdfs directory could't be created
* [SPOI-2060] - UI Shows Nan B in Allocated Memory
* [SPOI-2062] - Gateway needs to check hadoop version
* [SPOI-2064] - dtgateway-ctl stop doesn't work
* [SPOI-2065] - Readme File is not udpated
* [SPOI-2066] - installer not recognizing -l option
* [SPOI-2070] - Installer: echo_success command doesn't work with Ubuntu
* [SPOI-2073] - Invalid Ip Address in Installer UI
* [SPOI-2074] - install script needs to check hadoop version
* [SPOI-2075] - lauch-local-demos
* [SPOI-2076] - Licensing agent RPC gives NPE
* [SPOI-2077] - Installer: we need a separate page for hadoop installation directory 
* [SPOI-2078] - Change the certification and benchmarking script and code to use the new location of benchmarking apps
* [SPOI-2083] - install script, when run by rpm, complains about invalid group dtadmin
* [SPOI-2088] - Map Reduce demo applications still show the classnames when listed using launch-demos
* [SPOI-2089] - demo applications displayed after running the launch-demos command should be in alphabetical order
* [SPOI-2090] - Error while requesting evaluation license from Datatorrent.com
* [SPOI-2094] - Installer throws failed message while stopping gateway 
* [SPOI-2097] - launch-demos macro not available after installing the platform from self extracting intaller
* [SPOI-2098] - App names still have full classpath
* [SPOI-2120] - Installer - Restart Modal is not closed after Restart Failed (Happened Once)
* [SPOI-2132] - Ensure HDFS does not blow up with millions of files per sec
* [SPOI-2133] - Delete old files to ensure NN does not crash
* [SPOI-2134] - Send POC1 to customer
* [SPOI-2143] - Spelling error and reference to $HOME/.datatorrent
* [SPOI-2146] - Move Kafka benchmark apps to contrib folder
* [SPOI-2148] - Installer - Disable Closing Modals on Click
* [SPOI-2149] - Address the confusion around gateway address.
* [SPOI-2151] - User is not able to change the defaultFS during installation
* [SPOI-2153] - Cryptic error message when launching app on node0
* [SPOI-2157] - Getting logical plan returns error when one of the getters is bad
* [SPOI-2159] - gateway is polling resourcemanager for appinfo w/o subscriber
* [SPOI-2163] - Change directory before DTGateway launch
* [SPOI-2165] - Installer - add reload ability to System screen
* [SPOI-2170] - DTGateway classpath is duplicated after restart
* [SPOI-2171] - Remove reload button from System configuration screen
* [SPOI-2172] - Installer may display invalid port after starting DTGateway
* [SPOI-2173] - Installer base location change not working
* [SPOI-2192] - CLI command for getting list of operator classes from a jar
* [SPOI-2193] - CLI command for getting properties of a operator class from a jar
* [SPOI-2206] - dag view does not get rendered property in Firefox
* [SPOI-2227] - (Re)start license agent when license file is uploaded
* [SPOI-2229] - container local operators not redeployed
* [SPOI-2238] - Installer complains about sudo running as root
* [SPOI-2241] - DAG Firefox 28 Support


## Improvement
* [SPOI-1311] - Review platform documentation
* [SPOI-1567] - Certify against commercial Hadoop distributions
* [SPOI-1769] - Trying to kill a non dt app can return a better message
* [SPOI-1844] - Ship project-template with a log4j.properties file with debugging level set to INFO
* [SPOI-1852] - WebSocket client recovery logging
* [SPOI-1853] - Create WebSocket clients in containers on demand
* [SPOI-1854] - Option to retrieve only running and recently-ended apps
* [SPOI-1856] - Need something that is like LicensingAgentClient but not specific for stram.
* [SPOI-1857] - Gateway to warn about available licensed memory being low
* [SPOI-1858] - CLI to directly connect to license agent to get live license info
* [SPOI-1859] - Gateway to directly connect to license agent to get live license info
* [SPOI-1861] - Gateway command to restart itself
* [SPOI-1869] - Add UI build script to dist build file
* [SPOI-1874] - the first operator that stalls for more than specific period, take it out so as to unclog the processing
* [SPOI-1899] - Add appmaster container to container list
* [SPOI-1904] - Updates needed to the README file
* [SPOI-1911] - Run certification as part of nighty build
* [SPOI-1915] - Using $DT_HOME in README 
* [SPOI-1961] - Show the activation date of the license with list-license-agent command 
* [SPOI-1965] - The file demos.jar should be installed by default by the Installer
* [SPOI-1978] - Manual eval request (by e-mail) - template  
* [SPOI-1980] - DT Version in license request and generated license
* [SPOI-1981] - Approve / update license verification e-mail
* [SPOI-1997] - Certify against commercial Hadoop distributions
* [SPOI-2026] - Add support to LogicalPlan for testing dag serialization
* [SPOI-2150] - Update Readme file for the local install
* [SPOI-2155] - Installer - Validate Fields on blur event
* [SPOI-2156] - Installer - Navigation Code Cleanup
* [SPOI-2158] - Installer - CSS Classes
* [SPOI-2160] - Installer font size
* [SPOI-2166] - Configuration screen navigation panel
* [SPOI-2174] - Notify user with installation location and version
* [SPOI-2175] - Notify user about local DTGateway management during installation
* [SPOI-2188] - Installer - Register Instructions

## New Feature
* [SPOI-328] - Add annotation to declare the operator stateless
* [SPOI-393] - High Availability for STRAM
* [SPOI-868] - Setting operator properties as different types
* [SPOI-1182] - Add Key-based filter functionality to malhar library for Min, Max, SimpleMovingAverage, StandardDeviation like operators
* [SPOI-1654] - Logstream - aggregate top hits and bytes for URL, geo DMA, IP, URL/status code, url
* [SPOI-1747] - create a filter operator to output multiple records based on filter
* [SPOI-1756] - configuration for input adaptor
* [SPOI-1757] - configuration for filter operator
* [SPOI-1758] - configuration for dimension operator
* [SPOI-1759] - configuration for aggreagation operator
* [SPOI-1760] - configuration for web socket output
* [SPOI-1849] - add dt HDFS directory in configuration 
* [SPOI-1907] - Installer: HDFS directory creation attempt via Gateway (as part of config updates) 
* [SPOI-1909] - Port re-selection by Gateway if 9090, 9091, 9092, etc are taken
* [SPOI-1913] - Automate verifing the app memory for the demos
* [SPOI-1928] - gateway needs to be able to start with standalone hadoop jar (without hadoop installation)
* [SPOI-1929] - /ws/v1/about to include java version and hadoop location
* [SPOI-1930] - New installation script
* [SPOI-1966] - dtcli should be enabled to list app names (if available) as opposed to app class path
* [SPOI-1974] - Add throughput,  totalTuplesProcessed and elapsed time to performance benchmarking
* [SPOI-1975] - Display throughput, tuplesProcessed per sec and latency in a tabular format.
* [SPOI-1987] - Copy License to Front-End Distribution
* [SPOI-2003] - Verify and add all the demos except Machine data to app memory usage automation
* [SPOI-2011] - Make a separate jar file for performance benchmarking demos
* [SPOI-2013] - Support for doc link as an attribute for the application
* [SPOI-2018] - Have a launch-performance command in dtcli
* [SPOI-2021] - Rename all the apps under contrib to have meaningful names
* [SPOI-2023] - Make a launch-contrib command available in stram cli
* [SPOI-2027] - Packaging benchmarking demos
* [SPOI-2042] - redirect user to welcome screen if dt.configStatus is not "complete"
* [SPOI-2044] - set property dt.configStatus to "complete" when the user has completed the config wizard
* [SPOI-2122] - Installer - Offline Email Template
* [SPOI-2147] - Provide separate dt benchmarking package scripts to throughput and hdfs operators benchmarking
* [SPOI-2195] - Gateway REST API to return operator classes in a jar given superclass (or not)
* [SPOI-2196] - Gateway REST API to return properties of an operator in a jar 
* [SPOI-2200] - Installer - License Flow

## Story
* [SPOI-1608] - Platform Benchmarking (Platform1 and Platform2)

## Task
* [SPOI-722] - Document ads demo (add comments to codebase)
* [SPOI-1403] - HDFS Operator Benchmark
* [SPOI-1411] - Deprecate old Malhar webdemos once logstream is available
* [SPOI-1513] - datatorrent.com webapp development - app testing
* [SPOI-1610] - Develop benchmarking app for AdsDimension App (exactly once semantics) - Platform1
* [SPOI-1612] - Benchmarking Ads Dimension demo app on Morado cluster (exactly once semantics) - Platform1
* [SPOI-1618] - Benchmarking Machine Data app with Platform1
* [SPOI-1694] - Document DT SandBox preparation
* [SPOI-1730] - Default License in git needs to be replaced by the license cut by the real key 
* [SPOI-1732] - Create the real public/private key for licenses and store the private key in a safe place.
* [SPOI-1745] - Document Demo Application Setup
* [SPOI-1788] - CLI commands for licensing
* [SPOI-1794] - Create license info as a string 
* [SPOI-1796] - Soft enforcement for normal paid app. 200% bump?
* [SPOI-1848] - Gateway to support changing and getting config parameters
* [SPOI-1851] - Document virtual cluster setup
* [SPOI-1862] - Working on creating Wire Frames for the Installation of DT Platform
* [SPOI-1863] - Make installer work w/o Maven
* [SPOI-1865] - Allow user to configure application classpath
* [SPOI-1868] - Support Book Keeping in the HDFSStorage
* [SPOI-1870] - Validate dtcli generate-license-request
* [SPOI-1872] - Modify generatelicense process
* [SPOI-1873] - License process via console
* [SPOI-1876] - Application Developer Guide Improvements
* [SPOI-1877] - Download and build JDK Standrd Doclet Source as part of DT 
* [SPOI-1878] - DT Console Web UI Testing on Chrome
* [SPOI-1879] - DT Console Web UI Testing for Demo Apps on Chrome
* [SPOI-1884] - Operater developer guide review 
* [SPOI-1891] - Add allatori documentation
* [SPOI-1892] - Add automatic build of front components to distribution
* [SPOI-1893] - Quick Start Guide
* [SPOI-1914] - cli get-app-info to include info from hadoop
* [SPOI-1926] - Call license web service and return license file
* [SPOI-1927] - Returns license request blob for UI to assemble mailto link
* [SPOI-1939] - Twitter Top Counter Demo Applications Guide
* [SPOI-1940] - RPM packaging for installer
* [SPOI-1941] - Include demo UI into installer
* [SPOI-1942] - Remove Allatori code expiration for GA
* [SPOI-1943] - include more info in the license request 
* [SPOI-1945] - Add REST call to gateway to post license file
* [SPOI-1946] - virtual cluster configuration changes
* [SPOI-1947] - Create license@datatorrent.com
* [SPOI-1949] - Java application (with main method) that returns information given a license request blob
* [SPOI-1956] - License generation key pair expiration / private key protection
* [SPOI-1963] - Evaluate Doclava Doclet from Google
* [SPOI-1982] - E-mail verification success web page 
* [SPOI-1988] - Review Quick Start Guide
* [SPOI-1989] - AdsDimension - Demo Applications Guide
* [SPOI-1990] - Twitter Rolling Top Words Counter - Demo Applications Guide
* [SPOI-2004] - Installer testing for GA
* [SPOI-2006] - Grant Google Analytics Access To Following People
* [SPOI-2010] - Configure the apps to use minimum memory as verified by app-memory-usage-automation.
* [SPOI-2015] - Get Machine data into contrib.jar
* [SPOI-2016] - Fraud Detection in contrib.jar
* [SPOI-2017] - Quick Start Guide version 2
* [SPOI-2019] - List NxN performance apps (different event size vs different stream locality
* [SPOI-2025] - Getting Start Guide - Launch this copy.
* [SPOI-2032] - Certify Cloudera CDH 5.0
* [SPOI-2048] - Uninstall script
* [SPOI-2050] - Start gateway as service flag
* [SPOI-2069] - Test Installer
* [SPOI-2091] - Update installation license agreement
* [SPOI-2093] - Verify demo UI is bundled with installer
* [SPOI-2100] - For terms of service box, change "continue" to "accept and continue"
* [SPOI-2106] - Change the message on 1 yr registration
* [SPOI-2107] - Change the message on 1 yr registration
* [SPOI-2108] - Put timeglass and "loading" or spinning.... while Hadoop system properties are being loaded
* [SPOI-2109] - Gateway down creates bad error message
* [SPOI-2110] - Remove errors popping on the right hand of console
* [SPOI-2111] - If gateway outage is discovered add a message to get them back
* [SPOI-2112] - Change the message on Hadoop screen
* [SPOI-2114] - Error if HDFS does not exist
* [SPOI-2115] - Create a list of issues summary screen
* [SPOI-2116] - Show more instructions on the completed screen
* [SPOI-2131] - Ingestion POC
* [SPOI-2136] - Do cartesian products for key, val pair
* [SPOI-2142] - Allow customization of cartesian product 
* [SPOI-2161] - prereq message on welcome screen
* [SPOI-2162] - DFS error message
* [SPOI-2164] - DFS location validation
* [SPOI-2167] - Evaluate errors in Cloudera certification
* [SPOI-2223] - Test if all the applications with reduced container memory sizes run in sandbox 0.9.4.
* [SPOI-2230] - Uninstaller for RPM
* [SPOI-2231] - Provide Environment with Running Demos
* [SPOI-2240] - Set Up DataTorrent Demos on Dev Environment


## Sub-task
* [SPOI-1682] - Too many mbassy threads!!!
* [SPOI-1718] - Update HA Documentation
* [SPOI-1729] - Restore operator recovery checkpoints in AM recovery 
* [SPOI-1786] - Users should be able to generate license at datatorrent.com
* [SPOI-1931] - Installer - determine OS type and version
* [SPOI-1932] - installer with sudo/root user creation, service installs
* [SPOI-1933] - service wrappers for DTGateway
* [SPOI-1934] - HDFS directory creation during install
* [SPOI-1935] - Search for hadoop binaries standard paths
* [SPOI-1958] - Create HDFS Word input operator
* [SPOI-1962] - Add test to jekins nightly build
* [SPOI-1971] - Verify launch script for other apps from demo 
* [SPOI-1972] - When some app fail, the main monitor should still keep looking at the other apps
* [SPOI-2028] - Provide shell script 'benchmark-throughput' to produce a single summary table 
* [SPOI-2030] - Provide a list of individual demos used for benchmarking through dtcli benchmarking, so that the user can launch the demos individually.
* [SPOI-2031] - Package benchmarking suite into the installer and sandbox
* [SPOI-2046] - Update licenses location in installation script
* [SPOI-2053] - Certify CDH5.0 as part of Cloudera certification for inclusion in their process
* [SPOI-2054] - Certify installer on HW
* [SPOI-2079] - Run certification on bin install
* [SPOI-2085] - Change the certification resource xml files to contain the certification type.




## Bug
* [MLHR-729] - Columns in table wrap in Firefox
* [MLHR-730] - Dev server does not escape double-quotes in error message
* [MLHR-732] - Links Outline in Firefox
* [MLHR-754] - JarList and DepJarList headers point to nonexistent text items
* [MLHR-767] - "FINISHING" has no icon in status
* [MLHR-785] - NaN in file size column of jar list
* [MLHR-809] - Inconsistent landing page across browsers
* [MLHR-814] - Selecting operators in the logical operators widget does not activate any actions
* [MLHR-816] - Names of partitions in the partitions widget should not be shown as links
* [MLHR-817] - Clickin on 'outputPort' leads to PageNotFound error
* [MLHR-824] - Don't show license agent detail when license agent is not running
* [MLHR-826] - Dep Jars fail to load in specify dep jars modal
* [MLHR-830] - Optimize RandomWordInput operator used in perfromance benhcmarking to use local final variables to improve performance. 
* [MLHR-835] - Fix hard-coded file path in com.datatorrent.demos.wordcount.WordCountInputOperator
* [MLHR-836] - Need AbstractHDFSOutputAdapter
* [MLHR-863] - Add license headers to AbstractHdfsOutputOperator
* [MLHR-879] - Installer - Issues Management
* [MLHR-887] - Input operator that tails a growing log file in a directory
* [MLHR-899] - Give default name for all demo applications
* [MLHR-908] - Mark installation complete with dt.configStatus property
* [MLHR-911] - UI Shows Nan B in Allocated Memory
* [MLHR-912] - Installer - System Section - Show Field Specific Errors
* [MLHR-913] - Installer - System Section - Server-Side Error Messages
* [MLHR-918] - Yahoo finance with Alerts : Modify to accept multiple ticker symbols and remove hard-coded values.
* [MLHR-920] - Give desciptive names to benchmarking apps
* [MLHR-924] - Tail Operator should take care of the truncation of file
* [MLHR-930] - UI container list should not show time for last heartbeat for if the value is "-1"
* [MLHR-938] - gateway address property change
* [MLHR-939] - Put GatewayRestart into "actions" hash in settings.js
* [MLHR-940] - Delete the space in the name
* [MLHR-943] - Change the method name from isConnected to Connected in the db api
* [MLHR-991] - DAG Stream Locality


## Improvement
* [MLHR-731] - Compile LESS on the fly in dev environment
* [MLHR-735] - Remove unused bundling make commands, update README
* [MLHR-745] - Add icon to left widget manager drawer
* [MLHR-746] - Use "cores" not % cpu for cluster metric total cpu usage
* [MLHR-747] - Remove "#" for "number of container", et al labels
* [MLHR-748] - Only load all applications from RM on demand
* [MLHR-749] - Improve notification history pane
* [MLHR-750] - Normalize labels for everything in console
* [MLHR-751] - Add lock where close dashboard icon would be for default dashboard
* [MLHR-753] - Remove avg app age in cluster metrics
* [MLHR-755] - Change "max alloc mem" to "peak alloc mem" in cluster metrics
* [MLHR-756] - Add memory levels to tooltip of license mem gauge in top right
* [MLHR-768] - Remove beefy from npm shrinkwrap
* [MLHR-770] - Clean up BaseUtil, BaseModel, BaseCollection, add tests
* [MLHR-781] - shorten link to log file in container page
* [MLHR-798] - Add "config.adsdimensions.redis.dbindex" configuration in webdemo
* [MLHR-873] - Add equals and hashcode to JdbcOperatorBase
* [MLHR-931] - ETL: Create a converter api and provide an implementation for Json Object to flat map conversion
* [MLHR-935] - Have "silentErrors" option for models and collections
* [MLHR-937] - ETL: Create a unifier for DimensionComputation operator
* [MLHR-947] - Improve overall look and feel of install wizard
* [MLHR-948] - Remove mock issues from SummaryView in installer wizard
* [MLHR-993] - Demo UI - Default Applications Names for Application Discovery

## New Feature
* [MLHR-688] - Discard Real-Time Updates When Page is not Active
* [MLHR-757] - Dashboard - Save Widget Width to Local Storage
* [MLHR-759] - Dashboard - Widget Definitions Collection
* [MLHR-763] - Line Chart Widget - hAxis options
* [MLHR-764] - Dashboard App - Meteor Data Source
* [MLHR-765] - Shutdown Container Interface
* [MLHR-773] - Configuration wizard page
* [MLHR-788] - Web Demos - Redis and MongoDB Config
* [MLHR-789] - Web Demos - Fraud Demo MongoDB Database Name
* [MLHR-790] - Web Demos - Single Configuration File
* [MLHR-791] - Web Demos - Start Script
* [MLHR-792] - Web Demos - Single Page App
* [MLHR-793] - Web Demos - JS/CSS Bundle
* [MLHR-794] - Web Demos - WebSocket Pub/Sub
* [MLHR-795] - Web Demos - Resources Clean Up on Scope Destroy
* [MLHR-796] - Web Demos - Distribution Files
* [MLHR-797] - Web Demos - Running Instructions
* [MLHR-804] - Config Page (Manage Properties)
* [MLHR-805] - Web Demos - Distribution Package Instructions
* [MLHR-806] - Installer - License Requests (REST API Calls)
* [MLHR-807] - Installer - License Text
* [MLHR-818] - Installer - License Section
* [MLHR-819] - Installer - System Properties Section
* [MLHR-820] - Console - Node.js Dev Mode
* [MLHR-821] - Web Demos - Distribution Package Launch Script
* [MLHR-831] - Installer - License Flow
* [MLHR-834] - Installer - License - Registration
* [MLHR-858] - Installer - Gateway Restart
* [MLHR-861] - Installer - System Properties - IP List
* [MLHR-864] - Installer - Handling Hadoop Not Found
* [MLHR-903] - Installer - WebSocket DataSource Disconnect
* [MLHR-909] - Installer - Restart Confirmation
* [MLHR-914] - Installer - Error Messages
* [MLHR-919] - License Bar
* [MLHR-923] - Installer - Properties Update "Loading" Indicator
* [MLHR-925] - Installer Update
* [MLHR-934] - Allow overrides to $.().modal(options) for Modals

## Story
* [MLHR-705] - Node.js Pub/Sub Service
* [MLHR-708] - Evaluate Node.js Pub/Sub Services
* [MLHR-710] - Pie Chart Data Model
* [MLHR-712] - Dashboard App - Historical Data Support
* [MLHR-714] - Dashboard App - MongoDB Integration
* [MLHR-715] - Create Dashboard from Running App - Widgets Auto Discovery
* [MLHR-736] - Console Firefox Support
* [MLHR-737] - Console Safari Support
* [MLHR-801] - Installer/Config UI
* [MLHR-802] - Web Demos - Distribution Package
* [MLHR-803] - Installer (Wizard) Page

## Task
* [MLHR-349] - Add build script to Malhar/front
* [MLHR-402] - Logstream - aggregate top hits and bytes for URL, geo DMA, IP, URL/status code, url
* [MLHR-651] - Use compatible version of jersey/jackson/jetty in Malhar
* [MLHR-659] - Migrate MongoDB adapters to use the new database adapter interface
* [MLHR-741] - Web Apps (Demos) Firefox Support
* [MLHR-744] - Web Apps (Demos) Safari Support
* [MLHR-760] - Dashboard App - Meteor Integration
* [MLHR-761] - Dashboard App - Derby.js Integration
* [MLHR-762] - create install script for ui
* [MLHR-839] - Review guide on MachineData app
* [MLHR-842] - Demo guide for Pi Application
* [MLHR-843] - Demo guide for Twitter Top URL Counter demo application
* [MLHR-851] - Demo guide for Fraud detection demo application
* [MLHR-853] - Demo guide for Mobile demo application
* [MLHR-854] - Demo Guide for Word-Count Application
* [MLHR-855] - Demo guide for Pi Calculator application
* [MLHR-856] - Demo guide for Twitter Rolling Top Words Application
* [MLHR-859] - upgrade kryo to 2.23
* [MLHR-871] - Demo guide for Twitter Top URL Counter - Launch This Copy.
* [MLHR-872] - Demo Guide for Word-Count Application - Launch this copy.
* [MLHR-875] - Demo guide for Yahoo finance application
* [MLHR-876] - Demo guide for Yahoo finance alerting application
* [MLHR-877] - Demo guide for Yahoo finance application with Derby SQL
* [MLHR-892] - ETL logstream application - study the log stream application
* [MLHR-893] - ETL- Use the generic dimension operator that was created for a POC in Log stream 
* [MLHR-900] - ETL- Operators used by logstream application need to be generic and moved to library
* [MLHR-904] - Fix the nightly and trigger builds broken due to removal of api.codec and api.util
* [MLHR-905] - Dedup: Make deduper and bucket manager part of malhar library
* [MLHR-910] - Demo guide for Twitter Rolling Top Words Application - Launch This Copy
* [MLHR-915] - CLONE - Demo guide for Pi Application - Launch this copy
* [MLHR-916] - CLONE - Demo guide for Pi Calculator application - Launch this copy
* [MLHR-917] - CLONE - Demo guide for Mobile demo application - Launch this copy
* [MLHR-936] - Create new Redis Store using Lettuce redis client
* [MLHR-949] - Add confirmation to DTGateway restart button in System Properties
* [MLHR-962] - ETL : Create a sifter operator 
* [MLHR-980] - CLONE - Demo guide for Yahoo finance alerting application - launch this copy
* [MLHR-981] - CLONE - Demo guide for Yahoo finance application - launch this copy
* [MLHR-982] - CLONE - Demo guide for Yahoo finance application with Derby SQL - launch this copy

## Sub-task
* [MLHR-678] - Time Series Forecasting with Simple Linear Regression
* [MLHR-718] - Time Series Forecasting using Simple/Single Exponential Smoothing
* [MLHR-726] - Time Series Forecasting Operator using Holt's Linear Trend Model
* [MLHR-727] - Develop application for a telecom related use case for time series forecasting with Simple Linear Regression and CMA smoothing
* [MLHR-932] - Create Centered Moving Average Smoothing Operator




## Version 0.9.3

## New Feature
* [SPOI-261] - Design a general purpose read from stream and write to cassandra
* [SPOI-400] - Each streaming application should license check
* [SPOI-1622] - Input operator - XML parser
* [SPOI-1647] - LogStream UI 
* [SPOI-1770] - Gateway should expose list of available topics
* [SPOI-1778] - Open readme on sandbox startup
* [SPOI-1804] - Start license app on launch app if not running
* [SPOI-1805] - command to show license file info in cli
* [SPOI-1812] - Create REST call for specific license agent, given a license id
* [SPOI-1823] - Gateway REST API - Get Running Applications List
* [SPOI-1829] - Semantic URLs for Web Apps

## Improvement
* [SPOI-1202] - Provide a way to check whether an operator is partitioned
* [SPOI-1783] - Add allocatedMB to main application list
* [SPOI-1789] - Change frequency of heartbeats to license app
* [SPOI-1795] - License file to clearly state hard enforcement or soft enforcement
* [SPOI-1816] - support simple variable substitution in the cli
* [SPOI-1835] - support gateway status command

## Story
* [SPOI-1320] - Support MQTT protocol
* [SPOI-1542] - Input operator - Directory Scan

## Bug
* [SPOI-1696] - Make de-duper dynamically partitionable
* [SPOI-1704] - Stram enforcement to lock physical plan changes when license memory limit is reached
* [SPOI-1706] - Design a enforcement format for the license policy
* [SPOI-1711] - Encryption/obfuscation of communication between stram and license agent
* [SPOI-1779] - Update sandbox documentation terminology
* [SPOI-1802] - Provide total license memory via stram web services
* [SPOI-1807] - Unknown outage on Machine data demo
* [SPOI-1809] - Suppress expected error message in dtgateway-ctl  
* [SPOI-1810] - sample-stram-site.xml generates warnings
* [SPOI-1813] - Add SNAPSHOT repository to install pom
* [SPOI-1815] - Make stram memory reporting to license manager asynchronous 
* [SPOI-1818] - Chance "className" in license file to id
* [SPOI-1820] - dtcli script doesn't exit when maven command fails
* [SPOI-1826] - Update documentation title
* [SPOI-1827] - Install script errors
* [SPOI-1831] - CLI warning about trouble with license manager
* [SPOI-1833] - Use encrypted byte arrays for RPC wire protocol for licensing
* [SPOI-1840] - Change default license memory limit to 25GB
* [SPOI-1841] - Make stram memory enforcement tolerances property settings for the enforcer
* [SPOI-1842] - Investigate the possibility of engine obfuscation jar not containing any references to license package path
* [SPOI-1880] - GET nonexistent container returns 500 error

## Task
* [SPOI-1375] - All sandbox apps must work in 8G VM. Need to test each
* [SPOI-1467] - DB lookup for Cassandra
* [SPOI-1507] - datatorrent.com webapp development - pilot test of Angular and WP integration
* [SPOI-1509] - datatorrent.com webapp development - db design
* [SPOI-1511] - datatorrent.com webapp development - app design
* [SPOI-1512] - datatorrent.com webapp development - app dev
* [SPOI-1515] - datatorrent.com webapp deveopment - integrate standalone app with cms
* [SPOI-1516] - datatorrent.com webapp development - add GA info during registration
* [SPOI-1517] - datatorrent.com webapp development - background jobs
* [SPOI-1617] - Benchmarking Performance app with Platform2
* [SPOI-1641] - Benchmarking Ads Dimension App - Platform2
* [SPOI-1715] - Show remainingLicensedMB and allocatedMB in UI for each application
* [SPOI-1763] - Provide support for Accumulo NoSQL db
* [SPOI-1780] - Sandbox - activate license automatically
* [SPOI-1781] - Sandbox - increase memory to 8GB
* [SPOI-1782] - License App should use much less memory (256MB or less?)
* [SPOI-1787] - Add license instructions to README
* [SPOI-1790] - Ensure update to license app on any resource change by StrAM
* [SPOI-1791] - Hard enforcement for free license (6GB), and eval license
* [SPOI-1793] - Hide sub-license and make license object behave as “what is license data right now?”
* [SPOI-1798] - Date format change in license file
* [SPOI-1799] - Change name "Sublicense" to "Section" or "LicenseSection"
* [SPOI-1800] - Webservice specs for Gateway for license info
* [SPOI-1825] - Update end user documentation
* [SPOI-1832] - Support CDH default log4j setup

## Sub-task
* [SPOI-1451] - Show critical path
* [SPOI-1721] - Augment the Partitionable interface to inform of all the partitions which actually were deployed
* [SPOI-1733] - Container heartbeat RPC failover


## New Feature
* [MLHR-5] - UI component for license information
* [MLHR-6] - Google Line Chart Widget
* [MLHR-7] - Gauge Widget
* [MLHR-8] - Top N Widget
* [MLHR-9] - Compile Widget from HTML Template
* [MLHR-10] - Make widgets resizable and renameable in ui-builder
* [MLHR-11] - Dashboard Component Grunt Tasks
* [MLHR-653] - Dynamically Connect Widgets to WebSocket Topics
* [MLHR-655] - Create serializing mechanism for instantiated widgets and dashboard(s)
* [MLHR-656] - Add/Compile Widgets from templateUrl
* [MLHR-664] - Support MQTT protocol
* [MLHR-668] - Set up widget configure dialog
* [MLHR-669] - Visual Data Demo App
* [MLHR-671] - Add allocatedMB column in main application list
* [MLHR-673] - Dashboard App - Notification Service
* [MLHR-674] - Explicit Saving/Loading of Dash configurations in ui builder
* [MLHR-687] - Dashboard App - Filter WebSocket Topics
* [MLHR-689] - Dashboard App - Widget Options Modal
* [MLHR-690] - Dashboard App - Widgets Schema
* [MLHR-691] - WebSocket Topics Debugger Widget
* [MLHR-692] - JSON Widget
* [MLHR-693] - Progressbar Widget
* [MLHR-695] - Pie Chart Widget
* [MLHR-696] - Dashboard App - Development/Production Scripts
* [MLHR-697] - Dashboard App - Node.js Configuration
* [MLHR-698] - Dashboard App - WebSocket/REST API Configuration

## Improvement
* [MLHR-686] - Deglobalize the visibly component

## Bug
* [MLHR-4] - Use new livechart module for OpChart Widget
* [MLHR-13] - Console status column display
* [MLHR-648] - Update Issues section in README files 
* [MLHR-650] - Changing metrics on logical dag fails
* [MLHR-654] - Some widgets' height changes when changing width
* [MLHR-667] - Add UI version to console
* [MLHR-670] - Memory leak in console
* [MLHR-677] - Widgets Data Models
* [MLHR-680] - Update license information dialog for new REST call info
* [MLHR-725] - WindowId Formatter
* [MLHR-739] - Stream Locality Toggle fails for DAG view

## Story
* [MLHR-1] - Reusable Dashboard Component with AngularJS
* [MLHR-2] - Dashboard Widgets
* [MLHR-3] - Dashboard App

## Task
* [MLHR-321] - Directory Scan operator
* [MLHR-452] - Create a De-duplication operator
* [MLHR-603] - Supports upload of dependency jars
* [MLHR-638] - Test streaming application for dynamic partition
* [MLHR-645] - More fields in drop down in logicalDAG widget
* [MLHR-646] - Document issue tracking location in README
* [MLHR-652] - Parallel Simple Linear Regression
* [MLHR-657] - Migrate memcache adapters to use the new database adapter interface
* [MLHR-662] - Migrate Redis adapters to use the new database adapter interface
* [MLHR-663] - Design new DB adapters interface
* [MLHR-666] - DB lookup for Cassandra



## Version 0.9.2

## Bug
* [SPOI-1327] - AtLeastOnceTest.testInlineOperatorsRecovery intermittent failure
* [SPOI-1342] - DTCli should check license and relay the information with each application launched
* [SPOI-1383] - Last window id and recovery window id do not update on 0.9
* [SPOI-1439] - Gateway should be secured
* [SPOI-1445] - Add version detection for Gateway
* [SPOI-1456] - Free Memory in container widget changes too rapidly
* [SPOI-1540] - Specification of license handlers and enforcers in the license file.
* [SPOI-1632] - jar upload fails
* [SPOI-1634] - Uptime at 1Billion events/s (Machine data)
* [SPOI-1635] - Update node1 with latest machine data demo
* [SPOI-1676] - Incremental Obfuscation of dt-flume directory
* [SPOI-1677] - Supports uploading of dependency jars
* [SPOI-1678] - When loading jars, make sure they are in their separate space so they don't conflict with gateway, cli and other jars
* [SPOI-1679] - When uploading jar and when dependencies are not met, allow the upload with a message about dependencies
* [SPOI-1680] - gateway throws errors when retrieving web service info from stram
* [SPOI-1687] - Support launching jar and showing logical plan from HDFS 
* [SPOI-1688] - Map Reduce Monitor Does Not Publish WebSocket Data
* [SPOI-1697] - Update demo configuration on node2
* [SPOI-1703] - Update auto provisioning with DataTorrent 0.9.1 and GCE GA
* [SPOI-1707] - License agent should handle license expiry
* [SPOI-1708] - Stram should store license expiry
* [SPOI-1709] - Show license object information in gateway
* [SPOI-1710] - License cutting utility
* [SPOI-1712] - Gateway to gracefully handle stram being a newer version than itself
* [SPOI-1714] - Dynamic partition stop working if you start from only 1  partition
* [SPOI-1727] - ApplicationInfoAutoPublisher unit test error
* [SPOI-1728] - StramEvent exception prevents package name obfuscation
* [SPOI-1739] - recordingStartTime of operator stats is showing -1 from time to time
* [SPOI-1743] - Tuple recording on port is not showing up in web services
* [SPOI-1744] - Recording says ended even if the recording is still going on


## Improvement
* [SPOI-1098] - event recorder logging improvements
* [SPOI-1370] - Make the partition logic available to the end-users
* [SPOI-1448] - DAG Visualization - Stream Types
* [SPOI-1603] - BufferServerStatsCollection - dont check against bufferserverpublisher and subscriber
* [SPOI-1613] - Update the User Interface guide to reflect latest version (0.9.1)

## New Feature
* [SPOI-165] - Parent jira for authentication
* [SPOI-170] - Stream should authenticate before allowing an operator to connect
* [SPOI-258] - Develop Flume Sink and corresponding DT input adapter
* [SPOI-327] - Parent jira for Security
* [SPOI-401] - Licensing alert mechanisms
* [SPOI-411] - Ability to modify (add, upgrade, downgrade) license while the app is running
* [SPOI-436] - Provide Web Service for obtaining license information (Usage limits, etc)
* [SPOI-729] - Include license data in DT phone home
* [SPOI-872] - Logical View of Running Application
* [SPOI-975] - Support DataLocal Functionality
* [SPOI-1406] - Add log file path and/or URL to each container info map
* [SPOI-1621] - Input operator - CDR parser using CSV 
* [SPOI-1699] - Add locality (and maybe id?) to physical streams in REST calls

## Task
* [SPOI-1689] - Map Reduce Monitor Web App
* [SPOI-739] - Hadoop 2.2 certification
* [SPOI-763] - Competition study
* [SPOI-1140] - Annotate dag visualization with stream throughout and other data
* [SPOI-1246] - Support versioning for Gateway to STRAM communications 
* [SPOI-1253] - Create DataTorrent Application which provides licensing server functionality
* [SPOI-1389] - ContainerList view should show the log file name (stderr, stdout) in the info widget
* [SPOI-1405] - Design macros for node0 and node1
* [SPOI-1609] - Competitive analysis - DT (Platform1 and Platform2)
* [SPOI-1611] - Benchmarking Ads Dimension on Morado cluster (at-least-once semantics) - Platform1
* [SPOI-1616] - Benchmarking Performance app with Platform1
* [SPOI-1670] - Ensure that Dedup operator is fault tolerant
* [SPOI-1673] - use public/private key encryption for dt phone home
* [SPOI-1675] - Map Reduce Jobs
* [SPOI-1686] - Launch separate process when loading classes from application jars
* [SPOI-1722] - Create a utility to create default license
* [SPOI-1724] - Create a command line utility to generate customer license
* [SPOI-1736] - CLI warning on License Violation
* [SPOI-1742] - Update end-user documentation


## Sub-task
* [SPOI-919] - Certify secure mode with Hadoop 2.2.0
* [SPOI-966] - Create a licensing agent application
* [SPOI-1413] - Flume sink part 
* [SPOI-1414] - DT Input Adapter for Flume
* [SPOI-1475] - Augment Kafka operator to dynamically adapt to load and broker/partition changes
* [SPOI-1538] - Develop Ads Dimension on Morado cluster (at-least-once semantics) Platform1
* [SPOI-1713] - Secure communication between gateway and stram
* [SPOI-1720] - Ensure that the Partionable interface and StatsListener interface callbacks are made from the same thread
* [SPOI-1723] - The default license generation should be integrated with build
* [SPOI-1731] - Sync execution layer deployment state after recovery

## GitHub - DataTorrent/Malhar
* [616] - fix #615 Update Web Apps Instructions
* [615] - Update Web Apps Instructions
* [614] - corrected typo
* [613] - Fixes #599. Upload and specify dependency jars
* [612] - fixes #597
* [611] - fixes #610
* [610] - Telecom tests failing
* [609] - Github 597
* [608] - #fix 607 Machine Data Demo Day Format
* [607] - Machine Data Demo Day Format
* [606] - fixes #457 added xml parse operator to parse and pick values from xml nodes and att...
* [605] - added the history for hadoop 1.x
* [603] - Map Reduce Monitor - Elapsed Time
* [602] - Map Reduce Monitor - Elapsed Time
* [601] - Map Reduce Monitor - Bootstrap JS, Server Errors Notification, Header Alignment
* [599] - Provide UI for uploading and specifying dependency jars
* [598] - Map Reduce Monitor - Server Errors Notification
* [597] - Map Reduce Monitor App - CPU/Memory History
* [595] - Map Reduce Monitor (History Charts, Animations, Readme, AngularJS Upgrade)
* [594] - Map Reduce Monitor - Production Files (dist)
* [593] - Map Reduce Monitor - AngularUI Bootstrap Progressbar issue with ngAnimate
* [592] - Map Reduce Monitor Update (Readme, App List, History Charts)
* [591] - Map Reduce Monitor - Map/Reduce History Charts
* [590] - Fixes #401, adds zoom to physical DAG
* [589] - Map Reduce Monitor - App List Columns
* [586] - Map Reduce Monitor - App Id AngularJS Filter
* [585] - Fixes #569, Cosmetic changes
* [584] - Map Reduce Monitor - Show Active Job First
* [581] - Map Reduce Monitor Update (Loading Indicator, Animations, Delayed Promise)
* [580] - Map Reduce Monitor - AngularJS Animations
* [579] - Map Reduce Monitor - Upgrade to AngularJS 1.2.6
* [578] - Map Reduce Monitor - App List Loading Indicator
* [577] - fixes #553, fixes #575
* [576] - Map Reduce Monitor - AngularJS Delayed $q Promise
* [575] - Map Reduce Monitor App - Send Job Stats immediately on Subscribe Request
* [573] - Map Reduce Monitor Update (App List Grid, Progress Bars, Job Removal)
* [572] - fixes #570
* [571] - Map Reduce Monitor - Remove Job on WebSocket Message
* [570] - clipPath issue when multiple charts on the same page
* [569] - Various cosmetic updates for console
* [568] - fixes #542, tooltip no longer obstructed by graph lines
* [567] - Bird's Eye View for Physical DAG view
* [566] - fixes #544, windowIds can now handle initial value of -1 or 0
* [565] - Map Reduce Monitor - Job Selection
* [564] - fixes #357, added logical operator page
* [563] - Map Reduce Monitor - App List ng-grid
* [561] - Map Reduce Monitor - App List Table Filter
* [560] - Fixed exception with KryoSerializableStreamCodec #559
* [558] - CDR simulator #524
* [557] - Github 525
* [556] - set the name of the io threads created by ning asynchttpclient
* [554] - Squashed commit of the following:
* [553] - Map Reduce Monitor App - Store Map/Reduce Progress History
* [551] - fixes #550
* [550] - Map Reduce Monitor App - App Should Broadcast Special Message on Unsubscription
* [549] - Map Reduce Monitor - Stop Updates after Job Unsubscribe
* [548] - Map Reduce Monitor - Found Job Notification
* [547] - Fix Github #545
* [544] - Console does not handle initial windowId
* [542] - Tooltip for line graphs show up behind graphs after turning series on and off
* [541] - fixes #535
* [539] - Map Reduce Monitor - Merge Progress Bars with Progress Table
* [538] - Map Reduce Monitor - Combine Map/Reduce Counters
* [536] - Map Reduce Monitor - App List Running Jobs Progress Bar
* [535] - Console breaks when switching to other page
* [534] - Fixes #510, Unsubscribe Logical Operators when not used by any widget
* [533] - Fixes #521, Refactored WindowId usage
* [532] - Map Reduce Monitor - Counters
* [530] - Map Reduce Monitor - App List Sort
* [526] - Map Reduce Monitor - Counters
* [525] - CDR processing DAG prototype
* [523] - Github 512
* [521] - Normalize all WindowId objects by overriding "set" method of appropriate models
* [519] - Map Reduce Monitor - Header Alignment on Resize
* [518] - Map Reduce Web App
* [517] - Map Reduce Monitor - License Headers
* [516] - Map Reduce Monitor - Map Reduce Jobs List
* [515] - Map Reduce Monitor - AngularJS Modules Definition
* [514] - Map Reduce Monitor - Readme (Deployment and Running Instructions)
* [513] - Map Reduce Monitor - Job Query Loading Indicator
* [512] - Support Normalization Operator
* [511] - Map Reduce Monitor - AngularJS Settings Provider
* [510] - Unsubscribe logicalOperators on InstancePage when not in use by widget
* [509] - Fixes #505. Also removes one more instance of free memory metric for containers
* [507] - Map Reduce Monitor - Single Config (Server and Client)
* [505] - Add processed and emitted metrics to container overview widget
* [504] - fixes #356, container log url now available in container info widget
* [503] - Map Reduce Monitor - Active Job Highlight
* [502] - Map Reduce Monitor - AngularJS Parent Scope Event Propagation
* [501] - Fixes #364, removed free memory from container metrics
* [500] - Map Reduce Monitor - Mock Server
* [499] - Adding support for R. Basic operations - min, max and std deviation support added. Also adding support to run R scripts.
* [498] - Rsupport pull
* [496] - Map Reduce Monitor - Progress Line Chart
* [495] - Map Reduce Monitor - Running MAPREDUCE Applications Discovery
* [494] - fixes #420, can now set explicit height for widgets
* [493] - fix #488 added delay before reconnection
* [492] - CPU/RAM Metrics for Map Reduce Jobs
* [491] - fix #488 added delay before reconnection
* [489] - Map Reduce Monitor - Job Controller
* [488] - WebSocketOutputOperator should wait a specified number of seconds before reconnection
* [487] - Using uniform naming convention for applications. Fixed incorrect application names. Fixes #486.
* [486] - Application names are not uniform
* [485] - CPU/RAM Metrics for Map Reduce Jobs (Map Reduce Monitor App)
* [484] - Map Reduce Monitor - AngularJS UI-Router Nested Views
* [483] - Enhance the AbstractSlidingWindow #480, Add a SortedSlidingWindow operator #423
* [482] - fixes #411. bundling on server.js, monkeypatching fs to avoid EMFILE
* [479] - fix #443 reconnection when the connection is dropped
* [478] - fix #443 Handles reconnection when the connection is dropped
* [477] - Improvements to LogicalDagWidget. Fixes #399, #473, #475, #476
* [476] - Logical DAG Widget: Limit scroll scale extent
* [475] - Logical DAG Widget: add ability to reset initial dag view
* [474] - Map Reduce Monitor - AngularJS UI-Router
* [473] - Logical DAG Widget: only zoom when alt/option is held down
* [472] - Map Reduce Monitor App - App Does not Publish Completed Maps
* [471] - Map Redice Monitor - Reduce Progress Grid
* [470] - Map Redice Monitor - Map Progress Grid
* [469] - Map Redice Monitor - AngularJS Percentage Filter
* [468] - Map Reduce Monitor - Monitored Jobs Grid
* [467] - Add a general CSV parser operator to parse string, byte[] input to Map #451
* [466] - Map Redice Monitor - AngularJS Util Service
* [465] - Map Reduce Monitor - Unsubscribe Action
* [464] - Map Reduce Monitor - Client-Side Settings
* [463] - Map Reduce Monitor - WebSocket Unsubscribe
* [461] - Map Reduce Monitor -  Multiple Jobs Monitoring
* [460] - Map Reduce Monitor - Progress Bar Animation
* [459] - Map Reduce Monitor - Upgrade to AngularJS 1.2.4
* [458] - added xml parser operator and its test, fixes #457
* [456] - Github 444
* [454] - Map Reduce Monitor - AngularUI Bootstrap
* [453] - Map Reduce Monitor - Production Build with Grunt
* [452] - Map Reduce Monitor - jshint
* [451] - CSV input operator (CDR processing)
* [450] - Map Reduce Monitor - Progress Bars 
* [449] - Map Reduce Monitor App - WebSocket Query
* [448] - Map Reduce Monitor - Error Notifications with pnotify 
* [444] - Map Redice Monitor App - Publish Map/Reduce Updates as Array
* [443] - Map Reduce Monitor App WebSocket Issue
* [442] - Map Reduce Monitor - Node.js Proxy for Hadoop ResourceManager
* [441] - Map Reduce Monitor - REST Service
* [439] - Map Reduce Monitor - Server Configuration
* [438] - Map Reduce Monitor - Settings
* [436] - Map Reduce Monitor - Job Progress Grid
* [435] - Map Reduce Monitor - WebSocket Service with AngularJS provider
* [434] - Map Reduce Monitor - Unit Tests
* [433] - Map Reduce Monitor - AngularJS Directives (widgets)
* [432] - Map Reduce Monitor - Page Layout with Bootstrap
* [431] - Map Reduce Monitor - Node.js Server
* [430] - Map Reduce Monitor - Yeoman Generated App
* [428] - Normalization operator (CDR processing)
* [427] - Filter operator (CDR Processing)
* [426] - Enrichment operator (CDR processing)
* [425] - Aggregator operator (CDR processing)
* [422] - Github 421
* [421] - Create RedisOperator taking String,String for performance
* [420] - Allow widgets to have adjustable height
* [419] - DAG Styling, DAG Firefox Issue
* [418] - Logical DAG - Firefox Bottom Margin Issue
* [417] - Logical DAG Styling
* [416] - Fix jquery build error
* [415] - Organized scripts and server
* [414] - fix #408, fix #413 Logical DAG - Show Stream Locality on Demand
* [413] - Logical DAG - Right Aligned Legned and Show Locality Link
* [412] - Improve Front Dev Environment
* [411] - Improved dev environment for front
* [410] - Map Reduce Monitor Web App
* [409] - fix #393 Front Node.js Proxy
* [408] - Logical DAG - Show Stream Locality on Demand
* [407] - fixes #373
* [401] - Physical DAG - Smart Zoom
* [399] - Physical DAG - Bird's-Eye View
* [393] - Front Node.js Proxy
* [375] - fixes #367, improves reload time during dev on front
* [374] - fixes #367, improves reload time during dev on front
* [372] - Logical DAG Legend Styling
* [371] - Logical DAG Legend
* [370] - Fix issue316 issue317 pull
* [369] - Logical DAG - Legend
* [368] - Squashed commit of the following:
* [367] - Precompile templates for better dev process
* [366] - Documenting demos pull
* [365] - Normalized all "processed" and "emitted" labels
* [364] - Remove free memory from container metrics in UI
* [362] - Dependency to dagre-d3 fork
* [361] - Logical DAG - Stream Locality
* [360] - Update physical operators collection to fetch from physical plan
* [359] - Add source and sinks to physical operator list 
* [358] - Normalize processed/s emitted/s labels across data tables and dag view
* [357] - Create Logical Operator Page
* [356] - ContainerList view should show the log file name (stderr, stdout) in the info widget
* [355] - fixes #349, recently-launched app does not request operator list
* [354] - Make partitionable kafka input operator adjust partitions ifself for kafka partition change(del/add)
* [353] - Upgrade kafka to 0.8 release
* [352] - fixes #322
* [351] - Non-partitioned operators
* [350] - Key/Value lookup Storage Manager changes
* [349] - Application launch error in console
* [348] - Fixes #339, switches cluster metrics to websocket topic
* [347] - fix #346 Physical DAG - Remove Container IDs
* [346] - Physical DAG - Remove Container IDs
* [345] - Added sensible default display for avg app age field in cluster metrics widget #341
* [344] - Added build cmd to travis script, fixes #343
* [343] - Build step for front not in travis script
* [342] - Fix for #328
* [341] - Cluster overview display items (initial launch)
* [339] - Cluster stats should come from WebSocket topic
* [337] - Add 1 to N partition support for kafka input operator with simple kafka consumer #311
* [336] - logstream integration with siteops ui
* [332] - Squashed commit of the following:
* [329] - Documenting demos
* [328] - Numbers in graph overlay illegible when close together
* [325] - [DB Lookup & Caching] Create Mongo-db based DB lookup operator
* [323] - fixes #322
* [322] - Make connectionList property not null in Redisoutputoperator
* [313] - Marking operators non-partitionable #312
* [312] - Set partitionable to false for all operators which are cannot be partitioned
* [311] - Dynamic partition kafka input operator to adapt to real time load 
* [298] - LogStream - siteops conversion - totals per second ( pages / bytes)
* [297] - LogStream - siteops conversion - plot requests over time (per page / per server)
* [296] - LogStream - siteops conversion - top 10 servers with 404 response (req/sec)
* [295] - LogStream - siteops conversion - top 10 URL's with 404 response (req/sec)
* [294] - LogStream - siteops conversion - top 10 clients downloading (bytes/sec)
* [293] - LogStream - siteops conversion - top 10 client IP's (req/sec)
* [292] - LogStream - siteops conversion - server load (req/sec/server)
* [291] - LogStream - siteops conversion - top 10 URL's (req/sec)
* [290] - Kafka POM WARN during build
* [277] - Kafka input operator should destroy itself if the topic doesn't exist
* [270] - [DB Lookup & Caching] Improvement to the design of existing Storage Manager
* [269] - [logstream] integration with sitestats ui
* [111] - SPOI-1191 - #comment Added StandardDeviationKeyVal operator and test class.
* [104] - Map Reduce Monitor Application


## Version 0.9.1

## New Feature
* [SPOI-377] - Input adapter for CouchDB
* [SPOI-378] - Output Adapter for CouchDb
* [SPOI-410] - Document and test download to work with Cloudera CDH5
* [SPOI-945] - Add ability to specify application properties, DAG, operator and port attributes in stram configuration file.
* [SPOI-1088] - Add mouseover for new graphing module
* [SPOI-1172] - DAG Visualization - Partition Shape
* [SPOI-1215] - Gauges in Container View
* [SPOI-1407] - Add Jar File View
* [SPOI-1415] - De-duplicaton operator
* [SPOI-1483] - Hadoop management script or service
* [SPOI-1501] - Widget - logical operators
* [SPOI-1535] - Memory Gauge Widget
* [SPOI-1536] - CPU Gauge Widget
* [SPOI-1550] - Logical DAG - Multiple Metrics
* [SPOI-1556] - Logical DAG Real-Time Metrics - Tracking Model Changes
* [SPOI-1561] - Logical DAG - Throughput
* [SPOI-1562] - Logical DAG - Prev/Next Metric Navigation
* [SPOI-1600] - Logical DAG - Show Two Metrics
* [SPOI-1235] - Add support for "DB lookup" functionality 
* [SPOI-1236] - Provide support for Cassandra NoSQL database

## Bug
* [SPOI-600] - Twitter demo failed when I did two simulteneouce operator/container kill
* [SPOI-993] - Plan modification gives NPE when the containers aren't deployed yet
* [SPOI-1280] - Recording Chart widget shows up with "X"
* [SPOI-1303] - If the operator is setup sucessfully, ensure that it gets a chance to teardown.
* [SPOI-1331] - When an operator is undeployed, its input streams should be disconnected from any upstream operators.
* [SPOI-1388] - Nodes are showing IP instead of hostnames
* [SPOI-1412] - Buffer Server uses a different version of guava base jar than Malhar
* [SPOI-1427] - Accessing Demos From clean VM is not working
* [SPOI-1430] - When uploading jars, meta information should be extracted and return error if jar is not valid
* [SPOI-1433] - checkboxes in lists intermittently unresponsive
* [SPOI-1452] - GetApp web service returns 500 for apps killed via UI
* [SPOI-1457] - Kill container call returns 500 when request payload is not json
* [SPOI-1458] - After finish uploading jar from the development tab, the jar list does not get updated until browser's "refresh" button is pressed
* [SPOI-1459] - get recordings REST call returning null values
* [SPOI-1497] - Change the names of nodes in hadoop to be just hostname with the domain name.
* [SPOI-1498] - StreamList widget has "Name" for stream id
* [SPOI-1499] - CPU usage on operatorList widget is routinely over 100%, sometimes 900%?
* [SPOI-1548] - Logical DAG - Partition Count
* [SPOI-1555] - Logical DAG: Partition-Count Metric
* [SPOI-1563] - Moving average should be computed based on endWindow timestamps
* [SPOI-1570] - Update sandbox to support Hadoop 2.2 and DataTorrent 0.9.x
* [SPOI-1572] - gateway automatic update via websocket throws errors
* [SPOI-1590] - Datanode does not successfully connect to cluster after initial startup
* [SPOI-1614] - Front files not included during builds after migration to Malhar


## Improvement
* [SPOI-1005] - CLI: create a default clirc that is controlled by the installer
* [SPOI-1099] - Display app master implementation version
* [SPOI-1110] - BufferServer Stats should be passed as part of the port stats?
* [SPOI-1305] - Support StringCodec for enums natively in Attributes
* [SPOI-1306] - Reduce the verbosity of the Context interface by changing the requirement for defaultValue argument for attrValue
* [SPOI-1421] - Improve default name for widgets
* [SPOI-1453] - Publish recently ended apps on WebSocket topics along with running app(s)
* [SPOI-1560] - "ERROR: mux is missing ..." messages emitted during "mvn test" on stram
* [SPOI-1574] - Document the additions to the config file specification
* [SPOI-1582] - Color non-empty filter fields for emphasis
* [SPOI-1583] - Limit default number of rows for physical operators
* [SPOI-1588] - Create service wrapper for datanode, namenode, nodemanager, resourcemanager, and historyserver
* [SPOI-1592] - Latency Measure Units
* [SPOI-1596] - Service wrapper for DTGateway
* [SPOI-1601] - Create meaningful names to the demos instead of providing entire path as the demo names
* [SPOI-1605] - Make default dashes into logical and physical for Instance
* [SPOI-1606] - Remove various metrics from logical operator list

## Task
* [SPOI-907] - Create Application DAG visualization
* [SPOI-1002] - List application names instead of class names where available
* [SPOI-1140] - Annotate dag visualization with stream throughout and other data
* [SPOI-1237] - Handle multi-container failure
* [SPOI-1281] - Remove TopN Widget from dashboard
* [SPOI-1282] - Update benchmarks
* [SPOI-1297] - Recover from "multiple container" failure
* [SPOI-1321] - Automate datanode provisioning
* [SPOI-1354] - Figure out a way to have charts be dramatically different for key combinations
* [SPOI-1360] - Create an automation framework for DataTorrent cloud provisioning
* [SPOI-1365] - Add ability to create, delete, and bootstrap multiple nodes with a single command
* [SPOI-1385] - Reduce memory used for Twitter demo containers
* [SPOI-1411] - Deprecate old Malhar webdemos once logstream is available
* [SPOI-1431] - GET /ws/v1/jars should return more meta information
* [SPOI-1432] - Implement /ws/v1/jars/{jarname}/meta
* [SPOI-1473] - Show main cluster stats in DT Console
* [SPOI-1477] - Automate namenode provisioning
* [SPOI-1478] - Recipe for Google Cloud provisioning
* [SPOI-1479] - Base configuration and provisioning
* [SPOI-1480] - Java install recipe
* [SPOI-1481] - Recipe to install maven
* [SPOI-1482] - Hadoop install recipe
* [SPOI-1484] - Open source UI to Malhar
* [SPOI-1518] - DT Console AngularJS Integration
* [SPOI-1519] - Logstream - sitestats UI integration
* [SPOI-1521] - Gateway to return error details upon status code 400s and 500s
* [SPOI-1523] - Run certification tests on 0.9.0 release build
* [SPOI-1524] - Automate performance benchmarking for different stream localities and event sizes
* [SPOI-1526] - Update GC stram-site.xml to reflect new version settings and customizations
* [SPOI-1527] - Automate stream locality perfromance benchmarking with the same tuple 
* [SPOI-1541] - Create clirc compatible with demos
* [SPOI-1545] - Add documentation to chef repository
* [SPOI-1553] - DataTorrent performance testing on Google Cloud
* [SPOI-1554] - Create recipe for datatorrent local install
* [SPOI-1589] - Create cluster creation helper script for gc-nodes
* [SPOI-1591] - Create automatic installer for webapps
* [SPOI-1496] - Website framework selection
* [SPOI-1602] - Documentation index update to include Gateway API and Scalability Doc
* [SPOI-1607] - Update docs for 0.9.1 release

## Sub-task
* [SPOI-1447] - Show stream throughput
* [SPOI-1449] - DAG Visualization - Latency. Show latency per path/compute
* [SPOI-1450] - Show partitionable operators
* [SPOI-1464] - Allow operator to request/trigger repartitioning
* [SPOI-1466] - DB lookup for MongoDB
* [SPOI-1474] - Support operator specific stats in STRAM decisions
* [SPOI-1485] - Add license headers to all relevant files
* [SPOI-1486] - Move custom npm modules to node_modules folder, update npm registry
* [SPOI-1487] - Update .travis.yml in malhar to also build and test Front
* [SPOI-1488] - Create widget for displaying cluster info
* [SPOI-1490] - Expose gateway API calls for getting cluster information
* [SPOI-1529] - Understand exactly-once semantics. 
* [SPOI-1530] - Set-up cluster on local VM
* [SPOI-1531] - Understand existing AdsDimension App
* [SPOI-1532] - Develop similar app with at-least once semantics as AdsDimension App
* [SPOI-1534] - Setup on Morado cluster for bench-marking 
* [SPOI-1593] - Move front folder into Malhar repository
* [SPOI-1594] - Define code style standards, integrate a hinter
* [SPOI-1595] - Create tests for more js files

## GitHub - DataTorrent/Malhar
* [335] - Fix issue316 issue317 pull
* [331] - fix #330 Logical DAG Metrics
* [330] - Logical DAG Metrics
* [327] - Create a cluster stats widget
* [326] - Refactored DB Lookup operators and Cache Manager
* [324] - Add UI to repository
* [321] - Merged malhar dev 0.9.1 to logstream
* [320] - Squashed commit of the following:
* [316] - Provide a property to set tuple size in the RandomWord input operator
* [315] - fixes #314
* [314] - RedisOutputOperator Exception 
* [310] - Fixing default redis listen ports to 6379.  Fixes #309
* [309] - Fix default redis listen port in webapps config.js
* [308] - fixes #307
* [307] - Change the format for passing multiple redis instances during partition
* [305] - Fix github 278 and 283
* [304] - Added DataCache, JDBCBasedCache Operator and its test case #303
* [303] - [DB Lookup & Caching] Create an operator which caches database results in memory for specified time
* [301] - Removing unused dependency and changing default daemon port to 9090.
* [300] - Update default settings for webapps
* [299] - A stream app to benchmark kafka partitionable input operator
* [287] - Use Yammer Metric to tick the kafka consumer throughput
* [284] - Squashed commit of the following:
* [283] - Monitor throughput for each kafka consumer 
* [282] - Github 278 fix the Simple Kafka Consumer performance
* [278] - A performance bug in Simple kafka consumer
* [273] - Optimizing Redis Operator for inserting Maps
* [264] - Added DataCache, JDBCBasedCache Operator and its test case
* [262] - Marking operators non-partitionable
* [260] - Figure out a way to have charts be dramatically different for key combinations in Machine Demo
* [245] - Kafka Benchmark
* [159] - Squashed commit of the following:
* [147] - HighLow is not parameterized
* [130] - CouchDB Output Adaptar
* [289] - DimensionTimeBucket*Operator Usage and Issues
* [252] - Web Apps - Karma Unit Tests
* [251] - Machine Data Demo - Lookback Field Validation
* [250] - Mobile Locator Demo - Phone Range Support
* [232] - Show ingestion rate on MachineDemo UI
* [104] - Map Reduce Monitor Application
* [20] - Can not serialize Configuration Object


## Version 0.9.0

## Improvement
* [SPOI-437] - Create more granular RESTful API calls in Stram for various entities in an instance 
* [SPOI-446] - Compute aggregate values for application instance on server side
* [SPOI-465] - Increase unit test coverage to at least 50%
* [SPOI-683] - Document Active MQ operatos in library/io/ActiveMQ*
* [SPOI-1038] - Support more options in launch command
* [SPOI-1127] - Standardize "subscribe" commands for all models and collections
* [SPOI-1130] - Gateway to log requests and response (like access log in apache)
* [SPOI-1150] - Create new mrcolor instance on init of topN visualization
* [SPOI-1152] - CLI: add -v flag for debug logging
* [SPOI-1154] - Provide physical plan connections via the Daemon API
* [SPOI-1163] - Refactored pages and routing mechanism
* [SPOI-1187] - Allow 50% width for adjacent widgets
* [SPOI-1188] - Put widely used UI components in node_modules/datatorrent
* [SPOI-1224] - CLI Help enhancement
* [SPOI-1232] - Allow a way to assign a default value to an attribute while defining the attribute
* [SPOI-1295] - Make UI compatible with Backbone v1.1.0
* [SPOI-1333] - Show stream locality in Streams widget 

## New Feature
* [SPOI-66] - App wide "do not autorecord" parameter
* [SPOI-90] - Define and implement Apache server adapter node
* [SPOI-117] - Boolean operator node
* [SPOI-184] - Dashboard to have a page for one streaming application
* [SPOI-389] - Parent jira for "alerts" as a product
* [SPOI-444] - Provide a cascade merge unifier feature.
* [SPOI-711] - Ads Dimension Time Bucket Missing Key
* [SPOI-847] - Introduce Operator in Operator optimization (ThreadLocal)
* [SPOI-916] - Create upload jar file widget
* [SPOI-945] - Add ability to specify application properties, DAG, operator and port attributes in stram configuration file.
* [SPOI-1007] - Run tests with headless browser for CI
* [SPOI-1148] - Add CPU percentage to Operator List and Operator View
* [SPOI-1213] - Map Reduce Debugger - AngularJS - WebSocket Integration
* [SPOI-1227] - Map Reduce Debugger - JSHint 
* [SPOI-1248] - DAG Visualization - Physical View - Color Coding Containers
* [SPOI-1323] - Pre-Deployment hook for Operator
* [SPOI-1332] - Logical Plan Update According to new Daemon API
* [SPOI-1345] - Add Alert Page - Application Name
* [SPOI-1377] - Create central package for language items
* [SPOI-1398] - Update DAG View Tool to Daemon API 0.9
* [SPOI-1401] - Add AUTO_RECORD attribute on the operator level
* [SPOI-1402] - Add capability to look up past alerts by timestamp
* [SPOI-1235] - Add support for "DB lookup" functionality 

## Bug
* [SPOI-440] - Change throughput calculation to use endWindow time
* [SPOI-727] - Document demo for charts
* [SPOI-904] - Tupleviewer: jump to valid index greater than total - visible fails
* [SPOI-992] - Javascript operator scriptBinding not serializable
* [SPOI-1037] - Daemon is creating a lot of threads that are not doing anything.  Need to investigate and fix.
* [SPOI-1107] - Send the Recording information as part of appropriate OperatorStats or PortStats
* [SPOI-1164] - Mark non-partitionable operators on malhar
* [SPOI-1186] - CSS issue when a page is not using dash manager
* [SPOI-1189] - Misbehaving Mobile Locator Demo
* [SPOI-1195] - DAG Explorer POC
* [SPOI-1219] - Thread-Local streams should be marked as "inline"
* [SPOI-1220] - Create a certification test for adsdimension demo
* [SPOI-1221] - CLI should read configuration on app launch, not on start
* [SPOI-1249] - duplicate sinks in streams in physical plan returned by web service
* [SPOI-1283] - Fix mobile demo configuration in customer environment
* [SPOI-1290] - Support hadoop.socks.server setting for dtcli
* [SPOI-1298] - change product and package name and bump version 
* [SPOI-1299] - Documment current DT Phone Home data 
* [SPOI-1307] - Implement TupleRecorderCollection.deactivated method
* [SPOI-1308] - Document how to configure applications
* [SPOI-1318] - Logical DAG Display Issue in Firefox
* [SPOI-1324] - MobileDemo: NullPointerException in StramChild
* [SPOI-1327] - AtLeastOnceTest.testInlineOperatorsRecovery intermittent failure
* [SPOI-1328] - Container not released and no operators running in it
* [SPOI-1343] - Remove recordingNames from heartbeat
* [SPOI-1349] - support set-pager command in dtcli
* [SPOI-1351] - Need submit button and hour glass on machine data
* [SPOI-1368] - Recording never loads in tupleviewer
* [SPOI-1369] - Tests failing with new Daemon API changes
* [SPOI-1371] - Add Alert Page fails
* [SPOI-1374] - Drop twitter demo app from sandbox launch-demo jar
* [SPOI-1386] - If I click on a "finished" task, the left top nav bar says "widget" instead of "applications"
* [SPOI-1387] - If I click on a "finished" task, the left top nav bar says "widget" instead of "applications"
* [SPOI-1391] - CPU percentage for operators are 100x too large


## Task
* [SPOI-634] - Enable unit tests in CI
* [SPOI-716] - Compare original javascript with new script operators (Python, Bash)
* [SPOI-720] - Coding conventions for Malhar GitHub
* [SPOI-817] - Implement Log Input Operator
* [SPOI-818] - Implement Parser Operator
* [SPOI-824] - Document Daemon API
* [SPOI-852] - Add life time per app on main dashboard
* [SPOI-853] - Create Pie Chart widget based on D3.js
* [SPOI-854] - Create Bar Chart widget based on D3.js
* [SPOI-856] - Create Histogram (Real-Time Traffic) widget based on D3.js
* [SPOI-857] - Site Stats Back-End Front-End Integration
* [SPOI-861] - Redis Client with Node.js for Site Stats
* [SPOI-862] - REST API with Node.js for Site Stats
* [SPOI-907] - Create Application DAG visualization
* [SPOI-954] - Publish Platform REST API
* [SPOI-998] - Throughput computations should be controllable via attributes per application
* [SPOI-999] - Throughput computations should be controllable per operator
* [SPOI-1002] - List application names instead of class names where available
* [SPOI-1033] - Implement OiO validations
* [SPOI-1062] - Build out Google Cloud cluster
* [SPOI-1066] - Design Versioning Scheme for forward and backward compatiblity
* [SPOI-1086] - Dynamically scale up and down input Operators
* [SPOI-1131] - Create Training Sessions
* [SPOI-1133] - Create Training Session 2
* [SPOI-1134] - Create Training session 3
* [SPOI-1139] - Productize DAG visualization
* [SPOI-1142] - Build a fraud detection poc
* [SPOI-1143] - Implement new Daemon API
* [SPOI-1159] - Conform to new Daemon API for REST and WebSocket
* [SPOI-1160] - Move REST logic from DataSource to classes
* [SPOI-1165] - Create a certification test for performance demo
* [SPOI-1166] - Create a certification test for twitter demo
* [SPOI-1168] - Create a certification test for mobile demo
* [SPOI-1193] - replace web socket client in tuple recorder with async web socket client
* [SPOI-1194] - remove "sync" from hdfs part file collection and implement web socket update of the newest part file
* [SPOI-1197] - DAG View - Application Logical Plan as Standalone Java Application/Maven Plugin
* [SPOI-1198] - DAG View - DAG Visualization from JSON file
* [SPOI-1199] - DAG View - DAG Visualization from properties file
* [SPOI-1207] - Common Apps Template
* [SPOI-1210] - Map Reduce Debugger - AngularJS Directives
* [SPOI-1216] - Benchmark performance benchmark with stream locality
* [SPOI-1222] - CLI enhancements
* [SPOI-1223] - CLI enhancements
* [SPOI-1226] - Create list of platform attributes - port, operator, stream, application
* [SPOI-1239] - Names for deamon and cli
* [SPOI-1247] - Update UI for change in logicalPlan format
* [SPOI-1256] - Make Machine Data Demo scalable
* [SPOI-1262] - Parent JIRA for "face-lift demo apps for Hadoop World"
* [SPOI-1265] - Web Demos - Common Application
* [SPOI-1268] - Document all the attributes in Documentation (Guides)
* [SPOI-1276] - Customer demo build tools
* [SPOI-1277] - Add performance demo to customer apps
* [SPOI-1279] - In Stream List view show DataLocality
* [SPOI-1282] - Update benchmarks
* [SPOI-1285] - keys in instanceinfo widget to be shortened
* [SPOI-1286] - Add GB used to instanceOverview widget
* [SPOI-1287] - Rename operator names and stream names on AdsCustomerApplication
* [SPOI-1294] - Bump up the disk space in sandbox
* [SPOI-1309] - Update Operations and Installation Guide by adding a section on parameter setting
* [SPOI-1334] - Create data for Scalability White paper
* [SPOI-1335] - Create Scalability White paper
* [SPOI-1336] - Create First cut of logstream white paper
* [SPOI-1337] - Create first cut for Gateway white paper
* [SPOI-1339] - Productize LogStream white paper
* [SPOI-1340] - Write Scalalability and Design Pattern white paper
* [SPOI-1347] - Redis periodic cleanup node8
* [SPOI-1348] - Add elapsed time to demo throughput widget
* [SPOI-1352] - Need urls on Machine data demo to suppoer key combos
* [SPOI-1353] - Machine data demo UI should show which keys are not selected
* [SPOI-1356] - Make default look back on machine gen demo - 180 minutes
* [SPOI-1362] - Set up separate Redis instance and application for Machine demo
* [SPOI-1363] - gateway API recordings retrieval minor parameter name change
* [SPOI-1366] - Reduce scope of dependency versions in Front
* [SPOI-1367] - Name and ports not showing up in recording list
* [SPOI-1372] - On dashboard change "Operations" to "Applications" in the first tab
* [SPOI-1373] - Sandbox: size should be 8G by default, 4G causes most apps to not work
* [SPOI-1375] - All sandbox apps must work in 8G VM. Need to test each
* [SPOI-1381] - Add link to Malhar open source project in Sandbox
* [SPOI-1382] - Add MachineCustomerApplication to launch-customer jar on node1
* [SPOI-1399] - Add two new docs to the 0.9 release, to the website
* [SPOI-1400] - Add timestamp for each tuple in tuple recordings

## Sub-task
* [SPOI-1109] - Change the frontend code to so that the recording information is received as part of the stats
* [SPOI-1135] - Beautify "Twitter Top URLs" demo
* [SPOI-1156] - mocha-phantomjs fails on test
* [SPOI-1228] - Benchmark Performance with stream locality as thread-local
* [SPOI-1229] - Benchmark Performance with stream locality as process local
* [SPOI-1230] - Benchmark Performance with stream locality as node-local
* [SPOI-1231] - Benchmark Performance with stream locality as rack-local
* [SPOI-1240] - Create Hadoop 2.2 cluster on GC
* [SPOI-1241] - Migrate morado cluster to Hadoop 2.1
* [SPOI-1263] - Beautify "Mobile" demo app
* [SPOI-1264] - Beautify "Machine Generated" demo app
* [SPOI-1272] - Investigate Google Cluster Disks, Networking, Firewalls
* [SPOI-1273] - Install Chef Server for central provisioning
* [SPOI-1274] - Twitter demo - small changes in backend
* [SPOI-1275] - Mobile demo - small changes in backend
* [SPOI-1278] - Configure and install node0
* [SPOI-1304] - Migrate core to Hadoop 2.2
* [SPOI-1322] - Configure and install datanode
* [SPOI-1379] - Configuration changes to support Hadoop 2.2
* [SPOI-1380] - Fix customer demos creation for Hadoop 2.2
* [SPOI-1394] - Enable testing during build for Malhar
* [SPOI-1395] - Enable build testing for front
* [SPOI-1396] - Enable build testing from Core

## GitHub - DataTorrent/Malhar
* [288] - Machine Demo Circular Keys. Squashed commit of the following:
* [286] - Ads Dimensions Demo - REST API
* [285] - Machine Data Demo - No Data Behavior 
* [281] - Machine Data Demo - Circular Keys Retrieval Unit Test
* [280] - Machine Data Demo - Retrieving Circular Keys from Redis
* [276] - Make topic and brokerList configurable properties for kafka input operator
* [275] - fixed github issue #273
* [274] - Couch operators 0.9
* [267] - [logstream] add logstream usecases
* [263] - Backend support to add a range of mobile numbers 
* [261] - Github issues #260 and #240
* [259] - Make benchmarks appear more friendly in application list
* [258] - Ads Dimensions Demo. Squashed commit of the following:
* [257] - Machine and Ads Demos Common Styling
* [256] - SPOI-1408 added AsyncHttpClient.close() calls
* [255] - Github 240
* [254] - Ads Dimensions Demo - Redis Unit Test
* [253] - Port Ads Dimensions Demo to AngularJS
* [249] - Added support to mobile app for adding range of numbers at a time #242
* [248] - Remove reload button from MachineData demo
* [247] - Web Apps Upgrade to Daemon API 0.9. Machine Demo optimizations. Squashed commit of the following:
* [246] - Documentation had a typo which was repeated. I fixed it and also reorganized imports.
* [243] - Web Apps - Upgrade to Daemon API 0.9
* [241] - Add ads demo to common demo UI
* [240] - Make the keys in Machine Data circular
* [239] - Machine Demo - Negative Values in Random Data
* [238] - Machine Demo - Load Indicator on Slow Response
* [237] - Machine Demo - Instant Reload on Dimensions Change
* [236] - Machine Demo - Line Chart Options for Empty Chart
* [235] - Machine Demo - Server Polling Statistics
* [231] - fixed github issue #220
* [230] - Machine Demo - Error Handling
* [228] - Machine Demo - Redis Query Optimization
* [227] - Machine Demo - Client Cache for Chart Data
* [226] - fixed github issue 219
* [225] - 0.9 migration
* [224] - Show hourglass on the machine data demo in case of network (internet) being slow
* [223] - Setting Expiry Date for Redis Keys in RedisOutputOperator
* [222] - Couch Input Output Adaptors
* [221] - Web Apps - AngularJS Directive for Google Line Chart
* [220] - Making Redis Operator partionable
* [219] - Setting Expiry Date for Redis Keys in RedisOutputOperator
* [218] - made output port of alert escalation autorecord
* [217] - Add credit card fraud detection demo
* [216] - Squashed commit of the following:
* [215] - Machine demo should have 180 minutes as default in UI
* [211] - Fraud App Rename
* [210] - Web Apps - Fraud 
* [209] - 0.9 pull
* [208] - Web Apps - Elapsed Time
* [207] - 0.9 migration
* [206] - SimpleMovingAverage is resetting the second last window state
* [205] - Fix PythonOperator
* [204] - Fix BashOperator
* [203] - Marking operators non-partitionable
* [202] - added new output port in JsonByteArrayOperator for emiting flattened map
* [201] - #198 Machine Data Demo - Dynamic Dimensions. Squashed commit of the following:
* [200] - added new output port in JsonByteArrayOperator for emiting flattened map
* [199] - making the attributes configurable from stram-site
* [198] - Machine Data Demo - Dynamic Dimensions
* [197] - Web Apps Machine Demo Update/Multiple FIxes
* [196] - Mobile Demo corrections
* [195] - Web Apps Machine Demo Update/Multiple Fixes
* [194] - Web Apps - Machine Data Demo - Last Minute Calculations
* [193] - reduced the I/O worker thread multiplier to 1 as default in ning AHC
* [192] - Web Apps - Express Version
* [191] - Web Apps - Browser Dependencies
* [190] - 0.9 migration
* [188] - Web Apps (Demos) - Squashed commit of the following:
* [186] - Web Apps - Styling
* [185] - Web Apps - Readme
* [184] - made the machine data and ads demo scalable
* [183] - Web Apps - JSHint
* [182] - Fixed seeds phone generator and other bugs with MobileDemo #172
* [180] - suppress warning for stdout in console output operator
* [178] - Web Apps - License Headers
* [177] - Twitter demo application is called TwitterDevApplication
* [176] - Removed phone.html that is not being used. Fixes #175.
* [175] - Remove phone html that is not being used
* [174] - Resolving mobile demo bug
* [173] - bug in phonemovement
* [169] - Code format changes to KryoSerializableStreamCodec
* [168] - Web Apps - Demos Descriptions
* [167] - javascript operator now serializable
* [165] - Squashed commit of the following:
* [164] - Operator and stream names of adsdimension application can be clearer
* [163] - Web Apps - Mobile Locator Demo - Google Maps Marker Labels
* [162] - Web Apps - Machine Data Demo - CPU/RAM/HDD Gauges
* [161] - Web Apps - Machine Data Demo - Device ID Dimension
* [158] - Made Mobile enhancements #157
* [157] - Mobile Demo: Minor enhancements 
* [156] - Web Apps - Index Page
* [155] - Web Apps - Machine Data Demo
* [154] - Web Apps - Mobile Locator Demo
* [151] - change name and jump version
* [150] - Twitter multiplier variance #148
* [149] - Checkpoint consumer offset  #146
* [148] - Twitter demo: change tweet multipler from 100 to Random(90-110)
* [146] - Commit offset at checkpoint for kafka input operator
* [145] - Web Apps POC
* [144] - MachineData: bug in MachineInfoAveragingOperator
* [143] - migrated to ning for web socket interface provider
* [142] - 0.4 migration
* [141] - upgraded framework version to 0.3.5
* [140] - method rename in RedisOutputOperator #138
* [139] - Add partitionable kafka input operator #113
* [138] - Change selectDatabase in RedisOutputOperator to setDatabase
* [137] - Couch DB Output adapter added #130
* [136] - Reducing tuple blast size #135
* [135] - MachineData: reduce the number of tuples generated by the random generator
* [134] - Mapreduce Pull Request
* [133] - [logstream] logstream app flow
* [132] - Bug in UniqueKeyValCounter 
* [131] - Deleted old unique value count operator #129
* [129] - Delete the old UniqueValueKeyVal operator and rename the new UniqueValueCount 
* [128] - Corrections to UniqueValueCount #127
* [127] - UniqueValueCount improvements
* [126] - Changed deprecated call to setInline to setLocality.
* [125] - Changed setInline call to setLocality
* [123] - Clean up unused libraries
* [122] - squashed changed to UniqueCount #112
* [121] - Operator that counts unique value per key #112
* [120] - add all the locality modes to performance test
* [119] - Squashed changes and fixed version #99
* [118] - MachineData integrated operators #99
* [117] - Adding Redis input operators #43
* [116] - Logstream merge 43
* [115] - Adding Redis input operators
* [114] - Implement the automate partitioned kafka input operator(1:1) #113
* [113] - Add partitionable kafka input operator (1:1)
* [103] - move the test residue to maven target directory so that mvn clean can clean all the files
* [102] - Sqaushed changes for KryoSerializableStreamCodec & Test #95
* [101] - Pull request for #95
* [100] - Pull request for  issue #95
* [99] - Add more statistics calculations to machine data
* [98] - Issue 95
* [97] - Add machinedata demo
* [96] - Added output port to json byte array operator to emit JSONObjects too
* [95] - Create an Abstract SteamCodec which can be used for custom partitioning and uses kryo serialization
* [94] - added two output ports - outputMap, outputJSonObject
* [93] - Upgrade Kafka to 0.8
* [88] - starmcli not taking the jar passed during launching application
* [73] - Upgrade our kafka operator api to be compatible with mvn released Kafka version 0.8
* [70] - Clean kafka package in malhar library #53
* [52] - addCombination doesn't work in DimensionTimeBucketOperator
* [46] - SiteOps Dashboard Look and Feel
* [45] - Move SiteOps Demo to Node.js
* [44] - Sliding window aggregation computations operators
* [43] - Create Redis Input Operator
* [42] - Please pull my latest changes
* [41] - Web Demos - License Headers
* [40] - Move Ads Dimensions Demo to Node.js
* [39] - AlertEscalationOperatorTest test failing
* [38] - Move Mobile Demo to Node.js
* [37] - Move Twitter Demo to Node.js
* [36] - Move Twitter, Mobile and Ads Demo to Node.js - fixes #34
* [35] - Create AMQP Input Operator
* [34] - Move Twitter, Mobile and Ads Dimensions Demo to Node.js
* [33] - Ads Dimensions Demo - Common Template
* [32] - Common Assets for the Demos
* [31] - Migrate deprecated setInline calls. fixes #16
* [30] - bump master version to 0.3.5-SNAPSHOT
* [29] - Javascript operator scriptBinding not serializable
* [28] - Node.js Ads Dimensions Demo - "Play" mode
* [27] - Node.js Ads Dimensions Demo - Node.js Daemon
* [26] - Node.js Ads Dimensions Demo - Dynamic Port
* [25] - Node.js Ads Dimensions Demo - Readme
* [24] - Node.js Ads Dimensions Demo Rename
* [23] - Mrapplication
* [21] - keyvalpair doing hash on key and value
* [18] - 404 error in the UI for logicalPlan
* [17] - Emitted tuples by InputOperator incorrectly shown as zero
* [16] - Adapt new stream locality API 
* [11] - Create an alerts demo for testing alerts
* [4] - Make alert throttle operator window id based instead of wall clock time.


## Version 0.3.5

## Bug
* [SPOI-349] - InlineStream undeploy error
* [SPOI-766] - Process for certification of 0.3.3 launch on demo server
* [SPOI-953] - Implement Exactly Once
* [SPOI-959] - Operator Properties Widget
* [SPOI-976] - Mobile apps keep dying
* [SPOI-1013] - Cluster clocks are not in sync
* [SPOI-1017] - Daemon stopped publishing over WebSocket
* [SPOI-1022] - Alert is removed from the list on server failure
* [SPOI-1023] - "Method Not Allowed" error when deleting an alert
* [SPOI-1030] - DT Phone Home throw NullPointerException
* [SPOI-1034] - 404 Not Found when getting alerts
* [SPOI-1035] - Cannot create alerts with stramcli
* [SPOI-1041] - Operator Properties fail to load
* [SPOI-1051] - URLs templating for REST API
* [SPOI-1075] - Container planned/alloc does not update in real time
* [SPOI-1076] - Buffer server read is a flat line
* [SPOI-1081] - Get common urls for all demos
* [SPOI-1087] - Fix container chart
* [SPOI-1089] - Widgets initializing twice
* [SPOI-1090] - Add d3 to package.json deps
* [SPOI-1091] - dashboard manager css issue with background color
* [SPOI-1092] - Widget list not rendering on initial page load
* [SPOI-1093] - Memory leak when switching between pages
* [SPOI-1097] - dashboards not being saved
* [SPOI-1101] - Explore Kibana 3 as UI option for site stats
* [SPOI-1103] - Kill button doesn't work on applist palette
* [SPOI-1104] - Mode switch separator in header still visible
* [SPOI-1105] - shutdown application command requires empty data object
* [SPOI-1114] - Test for  AlertManagerTest.testAlertManager failed
* [SPOI-1118] - Upgrade grid to 4GB containers
* [SPOI-1121] - URL for deleting alert is incorrect
* [SPOI-1123] - Add alert action not sending all parameters
* [SPOI-1126] - Latency is whacked after adding alerts
* [SPOI-1137] - DAG Page View - JavaScript Error
* [SPOI-1144] - stram-site properties not applied to the operators (possibly?)
* [SPOI-1145] - ConcurrentModificationException when used launch local in stramcli
* [SPOI-1147] - Cannot kill "running" or relaunch "killed" application instance from AppLIst widget
* [SPOI-1157] - NoSuchElementException in stramcli

## Epic
* [SPOI-870] - Alerts parent jira for phase I

## Improvement
* [SPOI-462] - Feature to specify that an operator cannot be partitioned.
* [SPOI-523] - Document OS style guidelines
* [SPOI-750] - Performance Metrics Widget Unit Test
* [SPOI-937] - Incorporate jQuery UI into current dashboard
* [SPOI-962] - Daemon REST API Conventions
* [SPOI-963] - For exactly-once pick the correct checkpoint for recovery 
* [SPOI-979] - Need old versions of javadoc available online
* [SPOI-983] - Front-End Release Branch
* [SPOI-1019] - switch to jquery ui tooltip
* [SPOI-1028] - Correctly order output port list for operator AlertThreeLevelTimedEscalationOperator
* [SPOI-1078] - Add appInfo to apps.list WebSocket topic
* [SPOI-1094] - Subscribe to app-specific websocket topic while on application
* [SPOI-1102] - Alerts should be a list of js object literals
* [SPOI-1106] - tables should resize as widget resizes by default
* [SPOI-1119] - Remove Malhar subtree dependency from Core
* [SPOI-1125] - Utilize new "optional" attribute with alert template parameters

## New Feature
* [SPOI-641] - Setting partition on operator not supporting partitioning must result in error.
* [SPOI-810] - Create a line charting module using d3.js
* [SPOI-831] - Create REST API call for uploading JAR files
* [SPOI-832] - Create REST API call to retrieve all uploaded jar files
* [SPOI-834] - Create "Top N" widget
* [SPOI-874] - Provide description information for filter/throttle/action classes to be used by UI
* [SPOI-940] - Alert List Widget - Actions (Add/View/Delete)
* [SPOI-987] - Add stramcli tab completion for alias and macro
* [SPOI-989] - Operator Properties Widget - Data Access Logic
* [SPOI-1011] - AlertModel - Delete
* [SPOI-1012] - Mock Node.js Server - Alert Delete
* [SPOI-1014] - AlertCollection - Retrieve
* [SPOI-1016] - AlertModel - Create
* [SPOI-1018] - GitHub issue exporter for changelog
* [SPOI-1024] - AlertModel - Unit Test
* [SPOI-1025] - AlertCollection - Unit Test
* [SPOI-1026] - OpPropertiesModel - Unit Test
* [SPOI-1029] - Unit Testing - FakeXMLHttpRequest
* [SPOI-1040] - Node.js back-end for Ads Dimensions Demo
* [SPOI-1046] - Templatized Alert Creation Page
* [SPOI-1055] - Node.js Redis Client (Ads Dimensions Demo)
* [SPOI-1080] - Node.js Daemon
* [SPOI-1085] - Unified Demos Page
* [SPOI-1095] - Move Twitter Demo to Node.js
* [SPOI-1096] - Move Mobile Demo to Node.js
* [SPOI-1111] - Get common urls for all demos - POC
* [SPOI-1112] - Shutdown command from UI
* [SPOI-1116] - Move Machine Gen Demo to Node.js


## Task
* [SPOI-843] - Evaluate current market features
* [SPOI-846] - Implement exactly once
* [SPOI-855] - Dynamic re-partitioning happens on a snapshot, need to do a moving average by default
* [SPOI-895] - Implement REST API filterClasses for Alerts
* [SPOI-896] - Implement REST API escalationClasses for Alerts
* [SPOI-932] - CLI: support alert operations
* [SPOI-941] - Include latency in operator list in the dashboard.
* [SPOI-955] - Demo showing recurring payment check
* [SPOI-956] - Demo application for Machine (appliance) generated data monitoring resource usage (resources like CPU, RAM, etc)
* [SPOI-972] - Adapt core versioning scheme
* [SPOI-973] - Setup node.js as a supported technology
* [SPOI-982] - Version Eclipse settings
* [SPOI-986] - Version Nebeans settings
* [SPOI-995] - Unit test for AlertsManager
* [SPOI-996] - Unit test for AlertEscalation Operator
* [SPOI-1001] - Create a convenience script to start and stop hadoop cluster
* [SPOI-1008] - Cluster configuration versioning
* [SPOI-1009] - Need a signup link to Google group on Malhar first page
* [SPOI-1010] - Notificaitons on Malhar to include Google group
* [SPOI-1015] - Evaluate anomaly algorithms
* [SPOI-1021] - Set up user communication process
* [SPOI-1031] - Hide operations/development mode switch in UI
* [SPOI-1039] - Grid access
* [SPOI-1052] - Update node1 with latest release (0.3.4)
* [SPOI-1053] - Add users to grid
* [SPOI-1056] - Add create alert template REST call
* [SPOI-1057] - Change create alert REST call to use a template and parameters
* [SPOI-1058] - Retrieve alert REST call should include template name and parameters
* [SPOI-1060] - Get rid of stramRoot from the REST API (back end)
* [SPOI-1061] - Get rid of stramRoot from the REST API call (front end)
* [SPOI-1064] - Make machine generated data demo generic and launch on node1
* [SPOI-1070] - Dashboard look and feel for demos
* [SPOI-1071] - Dashboard look and feel for twitter firehose demo
* [SPOI-1072] - Dashboard look and feel for mobile demo
* [SPOI-1073] - Dashboard look and feel for ads dimensional demo
* [SPOI-1074] - Dashboard look and feel for machine gen demo
* [SPOI-1077] - Dashboard look and feel for logs demo
* [SPOI-1079] - Evaluate Integration with third party tools
* [SPOI-1100] - Update https://github.com/DataTorrent/Malhar
* [SPOI-1117] - Allow exactly-once with downstream at-most-once only
* [SPOI-1132] - Create Training session 1
* [SPOI-1151] - AngularJS Integration

## Sub-task
* [SPOI-876] - Allow for saving alert configurations as templates for future use, back-end
* [SPOI-877] - Allow for saving alert configurations as templates for future use, front-end
* [SPOI-978] - Fix dependency among open source and platform
* [SPOI-1042] - create alert template model
* [SPOI-1047] - create new page for add alert and re-route current url
* [SPOI-1048] - create parameter fill-in fields
* [SPOI-1113] - Remove child module poc build from framework build
* [SPOI-1176] - Remove malhar subtree from Core

## GitHub - DataTorrent/Malhar
* [90] - Fixed generics usage and renamed operator classes as per convention
* [89] - Added operator to convert json byte stream to java hash map
* [87] - Added operator to convert json byte stream to java hash map
* [86] - Web Demos Update
* [85] - Web Demos - Architecture Documentation
* [84] - Adding TravisCI build status to README #83
* [83] - Add TravisCI Build status to README
* [82] - Add TravisCI Integration
* [81] - [Logstream] Reads apache logs from RabbitMQ and prints basic aggregations to console
* [80] - Webdemos - round time to minute
* [79] - Adding apps project and logstream application skeleton.  #62
* [78] - SiteOps Demo - Totals Calculation
* [77] - Operator changes for issue #76
* [76] - Remove hardcoded values from RabbitMQ input operator
* [75] - [Issue 52]: addCombination doesn't work in DimensionTimeBucketOperator
* [74] - AMQP input operator for logs with sample aggregations #35
* [71] - Move Machine Generated Data Demo to Node.js
* [70] - Clean kafka package in malhar library #53
* [69] - Pull request for issue #53
* [68] - Web Demos Update
* [67] - SiteOps Demo - Redis Service
* [66] - Web Demos - Describe Configuration in Readme
* [65] - Ads Dimensions Demo - Redis Configuration
* [64] - Web Demos - Relative URLs for JS/CSS
* [63] - Adding apps project and logstream application skeleton.  #62
* [62] - Create skeleton application for log stream processing
* [60] - Move SiteOps Demo to Node.js - Unit Tests
* [59] - Move SiteOps Demo to Node.js - License Headers
* [58] - Move SiteOps Demo to Node.js - Charts
* [56] - Clean input operators in malhar-library
* [55] - Github 54
* [54] - PubSubWebSocket operators tests should be self contained and not need other helpers.
* [53] - Clean up Kafka input/output operator
* [51] - Ads webdemo is showing a drop in the graphs at the end
* [50] - Link to webdemos in main readme
* [49] - Clean input operators in lib
* [47] - Ads Dimensions Demo - JavaScript Loading Issue
* [22] - Node.js Ads Dimensions Demo
* [15] - Cleanup malhar-library input operator packages


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

