DataTorrent RTS Release Notes
========================================================================================================================

Version 3.1.0
------------------------------------------------------------------------------------------------------------------------

### Operator Improvements

* Fix Base Operator To Not Show Name Property In App Builder

### Few Bug Fixes

* Test exceptions due to missing directory in saveMetaInfo
* FSStorageAgent to account for HDFS lease when writing checkpoint files
* Container and operator json line file in StreamingContainerManager should not be appended from previous app attempt 
* SchemaSupport: TUPLE_CLASS attribute should use Class2String StringCodec
* Controlled plan modification on operator shutdown 
* Fix Base Operator To Not Show Name Property In App Builder


Version 3.0.0
------------------------------------------------------------------------------------------------------------------------

### Platform Enhancements

* Add jersey client jar for app execution
* Must catch NoClassDefFoundError when processing operator classes in jar, previously catching Throwable was changed to catching Exception
* Depend on published netlet version
* Catch throwable when executing command because launching an app can throw java.lang.VerifyError: bad type on operand stack
* Deploy all artifacts by default.
* HA support for stram webservice filter.
* Removed dependencies in filter to hadoop classes with private audience as their interface has changed from Hadoop 2.2 to 2.6
* Use tokens from Credentials as UserGroupInformation.getTokens() returns HDFS Namenode hidden tokens that should not be passed to containers.
* Support for RM delegation token renewal in secure HA environments
* Token authentication support for buffer server
* Adding default aggregator for primitive customMetrics
* Ability to extract javadocs as xml
* Switch to Java7 and update compiler plugin.
* Separated out HA token creation from non-HA case as it involves special handling that is subject to change if Hadoop's internals change.
* Changed the license header to Apache 2.0 license.



Version 2.0.0
------------------------------------------------------------------------------------------------------------------------

### Core platform enhancements
* Role based access control for secure installations
* LDAP authentication support
* Better control over application instances with new operator tuning options
* Numerous bug fixes

### Usability improvements
* Install and launch applications with Application Packages
* Installer enhancements to support various Hadoop distributions
* Angular JS based new management console
* Improvements to log file viewing and tuple recording features

### Hadoop Distributed Hash Table (HDHT)
* HDFS based distributed hash table
* Optimized for fast access to high volume of small size and potentially unstructured streaming data
* Does not require any additional software installation or configuration

### Malhar operator enhancements
* New operators for Elastic Search, Solr, and Kinesis
* Various fixes for fault tolerance, partitioning, and idempotency for existing operators

### Sandbox improvements
* Demo application import and launch inside Console with Application Packages
* Hadoop services validation and status notification prior to application launch
* Documentation improvements
