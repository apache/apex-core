Apache Apex Development Environment Setup
=========================================

This document discusses the steps needed for setting up a development environment for creating applications that run on the Apache Apex or the DataTorrent RTS streaming platform.


Microsoft Windows
------------------------------

There are a few tools that will be helpful when developing Apache Apex applications, some required and some optional:

1.  *git* -- A revision control system (version 1.7.1 or later). There are multiple git clients available for Windows ([git-scm][1] for example), so download and install a client of your choice.

2.  *java JDK* (not JRE). Includes the Java Runtime Environment as well as the Java compiler and a variety of tools (version 1.7.0\_79 or later). Can be downloaded from the Oracle website.

3.  *maven* -- Apache Maven is a build system for Java projects (version 3.0.5 or later). It can be downloaded from [Apache Maven][2].

4.  *VirtualBox* -- Oracle VirtualBox is a virtual machine manager (version 4.3 or later) and can be downloaded from [VirtualBox][3]. It is needed to run the Data Torrent Sandbox.

5.  *DataTorrent Sandbox* -- The sandbox can be downloaded from [Sandbox][4]. It is useful for testing simple applications since it contains Apache Hadoop and Data Torrent RTS 3.1.1 pre-installed with a time-limited Enterprise License. If you already installed the RTS Enterprise Edition (evaluation or production license) on a cluster, you can use that setup for deployment and testing instead of the sandbox.

6.  (Optional) If you prefer to use an IDE (Integrated Development Environment) such as *NetBeans*, *Eclipse* or *IntelliJ*, install that as well.


After installing these tools, make sure that the directories containing the executable files are in your PATH environment; for example, for the JDK executables like _java_ and _javac_, the directory might be something like `C:\\Program Files\\Java\\jdk1.7.0\_80\\bin`; for _git_ it might be `C:\\Program Files\\Git\\bin`; and for maven it might be `C:\\Users\\user\\Software\\apache-maven-3.3.3\\bin`. Open a console window and enter the command:

    echo %PATH%

to see the value of the `PATH` variable and verify that the above directories are present. If not, you can change its value clicking on the button at _Control Panel_ &#x21e8; _Advanced System Settings_ &#x21e8; _Advanced tab_ &#x21e8; _Environment Variables_.


Now run the following commands and ensure that the output is something similar to that shown in the table below:


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
<td align="left"><p>javac 1.7.0_80</p></td>
</tr>
<tr class="odd">
<td align="left"><p><tt>java -version</tt></p></td>
<td align="left"><p>java version &quot;1.7.0_80&quot;</p>
<p>Java(TM) SE Runtime Environment (build 1.7.0_80-b15)</p>
<p>Java HotSpot(TM) 64-Bit Server VM (build 24.80-b11, mixed mode)</p></td>
</tr>
<tr class="even">
<td align="left"><p><tt>git --version</tt></p></td>
<td align="left"><p>git version 2.6.1.windows.1</p></td>
</tr>
<tr class="odd">
<td align="left"><p><tt>mvn --version</tt></p></td>
<td align="left"><p>Apache Maven 3.3.3 (7994120775791599e205a5524ec3e0dfe41d4a06; 2015-04-22T06:57:37-05:00)</p>
<p>Maven home: C:\Users\ram\Software\apache-maven-3.3.3\bin\..</p>
<p>Java version: 1.7.0_80, vendor: Oracle Corporation</p>
<p>Java home: C:\Program Files\Java\jdk1.7.0_80\jre</p>
<p>Default locale: en_US, platform encoding: Cp1252</p>
<p>OS name: &quot;windows 8&quot;, version: &quot;6.2&quot;, arch: &quot;amd64&quot;, family: &quot;windows&quot;</p></td>
</tr>
</tbody>
</table>


To install the sandbox, first download it from [Sandbox][4] and import the downloaded file into VirtualBox. Once the import completes, you can select it and click the  Start button to start the sandbox.


The sandbox is configured with 6GB RAM; if your development machine has 16GB or more, you can increase the sandbox RAM to 8GB or more using the VirtualBox console. This will yield better performance and support larger applications. Additionally, you can change the network adapter from **NAT** to **Bridged Adapter**; this will allow you to login to the sandbox from your host machine using an _ssh_ tool like **PuTTY** and also to transfer files to and from the host using `pscp` on Windows. Of course all such configuration must be done when when the sandbox is not running.


You can choose to develop either directly on the sandbox or on your development machine. The advantage of the former is that most of the tools (e.g. _jdk_, _git_, _maven_) are pre-installed and also the package files created by your project are directly available to the Data Torrent tools such as  **dtManage** and **dtcli**. The disadvantage is that the sandbox is a memory-limited environment so running a memory-hungry tool like a Java IDE on it may starve other applications of memory.


You can now use the maven archetype to create a basic Apache Apex project as follows: Put these lines in a Windows command file called, for example, `newapp.cmd` and run it:

    @echo off
    @rem Script for creating a new application
    setlocal
    mvn archetype:generate ^
    -DarchetypeRepository=https://www.datatorrent.com/maven/content/repositories/releases ^
      -DarchetypeGroupId=com.datatorrent ^
      -DarchetypeArtifactId=apex-app-archetype ^
      -DarchetypeVersion=3.1.1 ^
      -DgroupId=com.example ^
      -Dpackage=com.example.myapexapp ^
      -DartifactId=myapexapp ^
      -Dversion=1.0-SNAPSHOT
    endlocal



The caret (^) at the end of some lines indicates that a continuation line follows. When you run this file, the properties will be displayed and you will be prompted with `` Y: :``; just press **Enter** to complete the project generation.


This command file also exists in the Data Torrent _examples_ repository which you can check out with:

    git clone https://github.com/DataTorrent/examples

You will find the script under `examples\tutorials\topnwords\scripts\newapp.cmd`.

You can also, if you prefer, use an IDE to generate the project as described in Section 3 of [Apex Packages](apex_package.md) but use the archetype version 3.1.1 instead of 3.0.0.


When the run completes successfully, you should see a new directory named `myapexapp` containing a maven project for building a basic Apache Apex application. It includes 3 source files:**Application.java**,  **RandomNumberGenerator.java** and **ApplicationTest.java**. You can now build the application by stepping into the new directory and running the appropriate maven command:

    cd myapexapp  
    mvn clean package -DskipTests

The build should create the application package file `myapexapp\target\myapexapp-1.0-SNAPSHOT.apa`. This file can then be uploaded to the Data Torrent GUI tool on the sandbox (called **dtManage**) and launched  from there. It generates a stream of random numbers and prints them out, each prefixed by the string  `hello world: `.  If you built this package on the host, you can transfer it to the sandbox using the `pscp` tool bundled with **PuTTY** mentioned earlier.


If you want to checkout the Apache Apex source repositories and build them, you can do so by running the script `build-apex.cmd` located in the same place in the examples repository described above. The source repositories contain more substantial demo applications and the associated source code. Alternatively, if you do not want to use the script, you can follow these simple manual steps:


1.  Check out the source code repositories:

        git clone https://github.com/apache/incubator-apex-core
        git clone https://github.com/apache/incubator-apex-malhar

2.  Switch to the appropriate release branch and build each repository:

        pushd incubator-apex-core
        git checkout release-3.1
        mvn clean install -DskipTests
        popd
        pushd incubator-apex-malhar
        git checkout release-3.1
        mvn clean install -DskipTests
        popd

The `install` argument to the `mvn` command installs resources from each project to your local maven repository (typically `.m2/repository` under your home directory), and **not** to the system directories, so Administrator privileges are not required. The  `-DskipTests` argument skips running unit tests since they take a long time. If this is a first-time installation, it might take several minutes to complete because maven will download a number of associated plugins.

After the build completes, you should see the demo application package files in the target directory under each demo subdirectory in `incubator-apex-malhar\demos\`.

Linux
------------------

Most of the instructions for Linux (and other Unix-like systems) are similar to those for Windows described above, so we will just note the differences.


The pre-requisites (such as _git_, _maven_, etc.) are the same as for Windows described above; please run the commands in the table and ensure that appropriate versions are present in your PATH environment variable (the command to display that variable is: `echo $PATH`).


The maven archetype command is the same except that continuation lines use a backslash (``\``) instead of caret (``^``); the script for it is available in the same location and is named `newapp` (without the `.cmd` extension). The script to checkout and build the Apache Apex repositories is named `build-apex`.

  [1]: http://git-scm.com/download/win
  [2]: https://maven.apache.org/download.cgi
  [3]: https://www.virtualbox.org/wiki/Downloads
  [4]: https://www.datatorrent.com/download
