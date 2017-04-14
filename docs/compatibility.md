#Apache Apex Compatibility

##Purpose

This document captures the compatibility goals of the Apache Apex project. The different types of compatibility between Apex releases that affect contributors, downstream projects, and end-users are enumerated. For each type of compatibility we:

* describe the impact on downstream projects or end-users
* where applicable, call out the policy adopted when incompatible changes are permitted.

Apache Apex follows [semantic versioning](http://semver.org/). Depending on the compatibility type, there may be different tools or mechanisms to ensure compatibility, for example by comparing artifacts during the build process.

The type of change will inform the required target version number. Given a version number MAJOR.MINOR.PATCH, increment the:

* MAJOR version when you make incompatible API changes,
* MINOR version when you add functionality in a backward-compatible manner, and
* PATCH version when you make backward-compatible bug fixes.

Additional labels for pre-release and build metadata are available as extensions to the MAJOR.MINOR.PATCH format.

The overall goal is to avoid backward incompatible changes and major release upgrades. Accordingly we attempt to release new features with minor versions that are incremental to the prior release and offer our users a frictionless upgrade path. When planning contributions, please consider compatibility and release road map upfront. Specifically, certain changes that conflict with the versioning may need to be documented in JIRA and deferred until a future major release. 

##Compatibility types

###Java API

Public API compatibility is required to ensure end-user programs and downstream projects continue to work without modification.
The public API consists of:

* apex-core: all interfaces and classes in `api` and `common` modules
* apex-malhar: all interfaces and classes in all modules except `demos`, `samples`, `benchmark` 

Interfaces and classes that are part of the public API and are annotated with [interface stability](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/InterfaceClassification.html) are treated according to the rules defined by the annotation.  

Policy

Changes to the public API must follow semantic versioning. 
Public APIs must be deprecated for at least one minor release prior to their removal in a major release.
The [japicmp Maven plugin](https://github.com/siom79/japicmp) is used to enforce compatibility as part of the Travis pre-commit builds.

###Semantic compatibility

The behavior of APIs needs to remain consistent over versions, though changes for correctness may result in changes in behavior. Tests and javadocs specify the behavior. Over time, test suites should be expanded to verify compliance with the specification, effectively creating a formal specification for the subset of behaviors that can be easily tested.

Policy

The behavior of existing API cannot be modified as it would break existing user code. There are exceptional circumstances that may justify such changes, in which cases they should be discussed on the mailing list before implementation. Examples are bug fixes related to security issues, data corruption/consistency or to correct an unintended change from previous release that violated semantic compatibility. Such changes should be accompanied by test coverage for the exact behavior.

###REST API

REST API compatibility corresponds to both the URLs and request/response content over the wire. REST APIs are specifically meant for stable use by clients across releases, even major releases. 

Policy

The REST API is separately versioned. This is to allow for co-existence of old and new API should there be a need for backward incompatible changes in the future.

###Command Line Interface (CLI)

The CLI may be used either directly via the system shell or via shell scripts. Changing the path, removing or renaming command line options, the order of arguments, or the command return code and output break compatibility and may adversely affect users.

Policy

CLI commands are to be deprecated (warning when used) in a prior minor release before they are removed or incompatibly modified in a subsequent major release.

###Configuration Files

Configuration files are used for engine or application settings. Changes to keys and default values directly affect users and are hard to diagnose (compared to a compile error, for example).

Policy

Name, location, format, keys of configuration files should be deprecated in a prior minor release and can only be changed in major release. Best effort should be made to support the deprecated behavior for one more major release (not guaranteed). It is also desirable to provide the user with a migration tool.

###Internal Wire compatibility

Apex containers internally use RPC communication and netlet for the data flow. The protocols are private and user components are not exposed to it. Apex is a YARN application and automatically deployed. There is currently no situation where containers of different Apex engine versions need to be interoperable. Should such a scenario become relevant in the future, wire compatibility needs to be specified.

Policy

N/A

###Internal File formats

Apex engine stores data in the file system for recovery and the data is typically obtained from serialization (from Kryo, Java etc.). Changes to internal classes may affect the ability to relaunch an application with upgraded engine code from previous state. This is currently not supported. In the future, the serialization mechanism should guarantee backward compatibility.

Policy

Currently no compatibility guarantee. User to cold-restart application on engine upgrade.

###Java Classpath

Apex applications should not bundle Hadoop dependencies or Apex engine dependencies but use the dependencies provided in the target environment to avoid conflicts. The Apex application archetype can be used to generate a compliant project.  

Policy

Apex engine dependencies can change as per semantic versioning. Following above guidelines automatically maintains the backward compatibility based on semantic versioning of Apex.

###Maven Build Artifacts

Downstream projects reference the Apex engine dependencies and Malhar operator libraries for application development etc. Changes to the packaging (which classes are in which jar), the groupId, artifactId and which artifacts are deployed to Maven central impact upgrades.

Policy

The artifacts that contain the classes that form the public API as specified above cannot change in patch releases and should stay compatible within a major release. Patch releases can change dependencies, but only at the patch level and following semantic versioning.

###Hardware/Software Requirements

Apex depends on Apache Hadoop. The community intends to support all major Hadoop distros and current versions. Apex currently supports Hadoop 2.6.0 and higher and Java 7 and higher. Apex is written in Java and has been tested on Linux based Hadoop clusters. There are no additional restrictions on the hardware architecture.

To keep up with the latest advances in hardware, operating systems, JVMs, Hadoop and other software, new Apex releases may require higher versions. Upgrading Apex may require upgrading other dependent software components.

Policy

The JVM and Hadoop minimum version requirements are not expected to change outside major releases.
