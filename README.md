

Apache Apex
========================
[![Master Build Status](https://travis-ci.org/apache/apex-core.svg?branch=master)](https://travis-ci.org/apache/apex-core/branches)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.apache.apex/apex/badge.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.apache.apex%22%20AND%20a%3A%22apex%22)

Apache Apex is a unified platform for big data stream and batch processing. Use cases include ingestion, ETL, real-time analytics, alerts and real-time actions. Apex is a Hadoop-native YARN implementation and uses HDFS by default. It simplifies development and productization of Hadoop applications by reducing time to market. Key features include Enterprise Grade Operability with Fault Tolerance,  State Management, Event Processing Guarantees, No Data Loss, In-memory Performance & Scalability and Native Window Support.

## Documentation

Please visit the [documentation section](http://apex.apache.org/docs.html). 

[Malhar](https://github.com/apache/apex-malhar) is a library of application building blocks and examples that will help you build out your first Apex application quickly.

Documentation build and hosting process is explained in [docs README](docs/README.md).

## Contributing

This project welcomes new contributors.  If you would like to help by adding new features, enhancements or fixing bugs, check out the [contributing guidelines](http://apex.apache.org/contributing.html).

You acknowledge that your submissions to this repository are made pursuant the terms of the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0.html) and constitute "Contributions," as defined therein, and you represent and warrant that you have the right and authority to do so.
 
## Building Apex

The project uses Maven for the build. Run 
```
mvn install
``` 
at the top level. You can then use the command line interface (CLI) from the build directory:
```
./engine/src/main/scripts/apex
```
Type help to list available commands. 

Pre-built distributions are listed on http://apex.apache.org/downloads.html

## Issue tracking

[JIRA](https://issues.apache.org/jira/browse/APEXCORE) issue tracking system is used for this project.
You can submit new issues and track the progress of existing issues at https://issues.apache.org/jira/browse/APEXCORE

Please include the JIRA ticket number into the commit messages. It will automatically add the commit message to the JIRA ticket(s) and help link the commit with the issue(s) being tracked for easy reference.
An example commit might look like this:

    git commit -am "APEXCORE-1234 Task completed ahead of schedule"

JIRA tickets should be resolved and fix version field set by the committer merging the pull request.

## License

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

## Contact

Please visit http://apex.apache.org and [subscribe](http://apex.apache.org/community.html) to the mailing lists.

