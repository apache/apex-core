Apache Apex Development Environment Setup
=========================================

This document discusses the steps needed for setting up a development environment for creating applications that run on the Apache Apex platform.


Development Tools
-------------------------------------------------------------------------------

There are a few tools that will be helpful when developing Apache Apex applications, including:

1.  **git** - A revision control system (version 1.7.1 or later). There are multiple git clients available for Windows (<http://git-scm.com/download/win> for example), so download and install a client of your choice.

2.  **java JDK** (not JRE) - Includes the Java Runtime Environment as well as the Java compiler and a variety of tools (version 1.8.0 or later). Can be downloaded from the Oracle website.

3.  **maven** - Apache Maven is a build system for Java projects (version 3.0.5 or later). It can be downloaded from <https://maven.apache.org/download.cgi>.

4.  **IDE** (Optional) - If you prefer to use an IDE (Integrated Development Environment) such as *NetBeans*, *Eclipse* or *IntelliJ*, install that as well.

After installing these tools, make sure that the directories containing the executable files are in your PATH environment variable.

* **Windows** - Open a console window and enter the command `echo %PATH%` to see the value of the `PATH` variable and verify that the above directories for Java, git, and maven executables are present.  JDK executables like _java_ and _javac_, the directory might be something like `C:\Program Files\Java\jdk1.8.0_162\bin`; for _git_ it might be `C:\Program Files\Git\bin`; and for maven it might be `C:\Users\user\Software\apache-maven-3.3.3\bin`.  If not, you can change its value clicking on the button at _Control Panel_ &#x21e8; _Advanced System Settings_ &#x21e8; _Advanced tab_ &#x21e8; _Environment Variables_.
* **Linux and Mac** - Open a console/terminal window and enter the command `echo $PATH` to see the value of the `PATH` variable and verify that the above directories for Java, git, and maven executables are present.  If not, make sure software is downloaded and installed, and optionally PATH reference is added and exported  in a `~/.profile` or `~/.bash_profile`.  For example to add maven located in `/sfw/maven/apache-maven-3.3.3` to PATH add the line: `export PATH=$PATH:/sfw/maven/apache-maven-3.3.3/bin`


Confirm by running the following commands and comparing with output that show in the table below:

<table>
<colgroup>
<col width="30%" />
<col width="70%" />
</colgroup>
<tbody>
<tr class="odd">
<td align="left"><p>Command</p></td>
<td align="left"><p>Output</p></td>
</tr>
<tr class="even">
<td align="left"><p><tt>javac -version</tt></p></td>
<td align="left"><p>javac 1.8.0_162</p></td>
</tr>
<tr class="odd">
<td align="left"><p><tt>java -version</tt></p></td>
<td align="left"><p>java version &quot;1.8.0_162&quot;</p>
<p>Java(TM) SE Runtime Environment (build 1.8.0_162-b12)</p>
<p>Java HotSpot(TM) 64-Bit Server VM (build 25.162-b12, mixed mode)</p></td>
</tr>
<tr class="even">
<td align="left"><p><tt>git --version</tt></p></td>
<td align="left"><p>git version 2.6.1.windows.1</p></td>
</tr>
<tr class="odd">
<td align="left"><p><tt>mvn --version</tt></p></td>
<td align="left"><p>Apache Maven 3.3.3 (7994120775791599e205a5524ec3e0dfe41d4a06; 2015-04-22T06:57:37-05:00)</p>
<p>...</p>
</td>
</tr>
</tbody>
</table>


Creating New Apex Project
-------------------------------------------------------------------------------

After development tools are configured, you can now use the maven archetype to create a basic Apache Apex project.  **Note:** When executing the commands below, you can optionally replace `RELEASE` with a [specific version](http://apex.apache.org/downloads.html) of Apache Apex.


* **Windows** - Create a new Windows command file called `newapp.cmd` by copying the lines below, and execute it.  When you run this file, the properties will be displayed and you will be prompted with `` Y: :``; just press **Enter** to complete the project generation.  The caret (^) at the end of some lines indicates that a continuation line follows. 

        @echo off
        @rem Script for creating a new application
        setlocal
        mvn archetype:generate ^
         -DarchetypeGroupId=org.apache.apex ^
         -DarchetypeArtifactId=apex-app-archetype -DarchetypeVersion=RELEASE ^
         -DgroupId=com.example -Dpackage=com.example.myapexapp -DartifactId=myapexapp ^
         -Dversion=1.0-SNAPSHOT
        endlocal


* **Linux** - Execute the lines below in a terminal window.  New project will be created in the curent working directory.  The backslash (\\) at the end of the lines indicates continuation.

        mvn archetype:generate \
         -DarchetypeGroupId=org.apache.apex \
         -DarchetypeArtifactId=apex-app-archetype -DarchetypeVersion=RELEASE \
         -DgroupId=com.example -Dpackage=com.example.myapexapp -DartifactId=myapexapp \
         -Dversion=1.0-SNAPSHOT


When the run completes successfully, you should see a new directory named `myapexapp` containing a maven project for building a basic Apache Apex application. It includes 3 source files:**Application.java**,  **RandomNumberGenerator.java** and **ApplicationTest.java**. You can now build the application by stepping into the new directory and running the maven package command:

    cd myapexapp
    mvn clean package -DskipTests

The build should create the application package file `myapexapp/target/myapexapp-1.0-SNAPSHOT.apa`. This application package can then be used to launch example application via **apex** CLI, or other visual management tools.  When running, this application will generate a stream of random numbers and print them out, each prefixed by the string `hello world:`.

Running Unit Tests
----
To run unit tests on Linux or OSX, simply run the usual maven command, for example: `mvn test`.

On Windows, an additional file, `winutils.exe`, is required; download it from
<https://github.com/srccodes/hadoop-common-2.2.0-bin/archive/master.zip>
and unpack the archive to, say, `C:\hadoop`; this file should be present under
`hadoop-common-2.2.0-bin-master\bin` within it.

Set the `HADOOP_HOME` environment variable system-wide to
`c:\hadoop\hadoop-common-2.2.0-bin-master` as described at:
<https://www.microsoft.com/resources/documentation/windows/xp/all/proddocs/en-us/sysdm_advancd_environmnt_addchange_variable.mspx?mfr=true>. You should now be able to run unit tests normally.

If you prefer not to set the variable globally, you can set it on the command line or within
your IDE. For example, on the command line, specify the maven
property `hadoop.home.dir`:

    mvn -Dhadoop.home.dir=c:\hadoop\hadoop-common-2.2.0-bin-master test

or set the environment variable separately:

    set HADOOP_HOME=c:\hadoop\hadoop-common-2.2.0-bin-master
    mvn test

Within your IDE, set the environment variable and then run the desired
unit test in the usual way. For example, with NetBeans you can add:

    Env.HADOOP_HOME=c:/hadoop/hadoop-common-2.2.0-bin-master

at _Properties &#8658; Actions &#8658; Run project &#8658; Set Properties_.

Similarly, in Eclipse (Mars) add it to the
project properties at _Properties &#8658; Run/Debug Settings &#8658; ApplicationTest
&#8658; Environment_ tab.


Building Apex Demos
-------------------------------------------------------------------------------

If you want to see more substantial Apex demo applications and the associated source code, you can follow these simple steps to check out and build them.

1.  Check out the source code repositories:

        git clone https://github.com/apache/apex-core
        git clone https://github.com/apache/apex-malhar

2.  Switch to the appropriate release branch and build each repository:

        cd apex-core
        mvn clean install -DskipTests

        cd apex-malhar
        mvn clean install -DskipTests


The `install` argument to the `mvn` command installs resources from each project to your local maven repository (typically `.m2/repository` under your home directory), and **not** to the system directories, so Administrator privileges are not required. The  `-DskipTests` argument skips running unit tests since they take a long time. If this is a first-time installation, it might take several minutes to complete because maven will download a number of associated plugins.

After the build completes, you should see the demo application package files in the target directory under each demo subdirectory in `apex-malhar/demos`.



Sandbox
-------------------------------------------------------------------------------

To jump-start development with Apex, please refer to the [Downloads](https://apex.apache.org/downloads.html) section of the Apache Apex website, which provides a list of 3rd party Apex binary packages and sandbox environments.
