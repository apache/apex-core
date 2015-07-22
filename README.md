

Welcome to Apex!
===============

Apex is a unified platform for big data stream and batch processing. Use cases include ingestion, ETL, real-time analytics, alerts and real-time actions. Apex is a Hadoop-native YARN implementation and uses HDFS by default. It simplifies development and productization of Hadoop applications by reducing time to market. Key features include Enterprise Grade Operability with Fault Tolerance,  State Management, Event Processing Guarantees, No Data Loss, In-memory Performance & Scalability and Native Window Support.

##Documentation

Please visit the [documentation section](https://www.datatorrent.com/developer-resources/). To get started with developing applications, have a look at [Application Packages Guide](https://www.datatorrent.com/docs/guides/ApplicationPackages.html) and [Application Developer Guide](https://www.datatorrent.com/docs/guides/ApplicationDeveloperGuide.html).

[Malhar](https://github.com/DataTorrent/Malhar) is the library of application building blocks and examples that will help you build out your first Apex application quickly.

##Contributing

This project welcomes new contributors.  If you would like to help by adding new features, enhancements or fixing bugs, here is how to do it.

You acknowledge that your submissions to this repository are made pursuant the terms of the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0.html) and constitute "Contributions," as defined therein, and you represent and warrant that you have the right and authority to do so.

  * Fork your own GitHub repository
  * Create a topic branch with an appropriate name
  * Write code, comments, tests in your repository
  * Create a GitHub pull request from your repository, providing as many details about your changes as possible
  * After review and acceptance one of the committers will merge the pull request.

When adding **new files**, please include the following Apache v2.0 license header at the top of the file, with the fields enclosed by brackets "[]" replaced with your own identifying information **(don't include the brackets!)**:
```java
/**
 * Copyright (C) [XXXX] [NAME OF COPYRIGHT OWNER]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
```
Thanks for contributing!
 
##Building Apex

Apex uses Apache Maven for the project management. To build and test the project:
```
mvn install
``` 
at the top level. You can then use the command line interface (CLI) from the build directory:
```
./engine/src/main/scripts/dtcli
```
Type help to list available commands. 

Pre-built distributions (see [README](https://www.datatorrent.com/docs/README.html)) are available from
https://www.datatorrent.com/download/

##Issue tracking

[Apex JIRA](https://malhar.atlassian.net/projects/APEX) issue tracking system is used for this project.
You can submit new issues and track the progress of existing issues at https://malhar.atlassian.net/projects/APEX.

When working with JIRA to submit pull requests, please use [smart commits](https://confluence.atlassian.com/display/AOD/Processing+JIRA+issues+with+commit+messages) feature by specifying APEX-XXXX in the commit messages.
It helps us link commits with issues being tracked for easy reference.  And example commit might look like this:

    git commit -am "APEX-1234 #comment Task completed ahead of schedule #resolve"

##License

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

##Contact

Use apex-dev@googlegroups.com to discuss ideas, ask questions about the implementation or any other communication while working on the engine code.  To subscribe or browse the message archive, visit https://groups.google.com/forum/#!forum/apex-dev
