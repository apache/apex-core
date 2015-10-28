#Apache Apex (incubating) Application Packages

# 1. Introduction

An Apache Apex Application Package is a zip file that contains all the
necessary files to launch an application in Apache Apex. It is the
standard way for assembling and sharing an Apache Apex application.

# 2. Requirements

You will need have the following installed:

1. Apache Maven 3.0 or later (for assembling the App Package)
2. Apache Apex 3.0.0 or later (for launching the App Package in your cluster)

#3. Creating Your First Apex App Package

You can create an Apex Application Package using your Linux command
line, or using your favorite IDE.

##Using Command Line

First, change to the directory where you put your projects, and create
an Apex application project using Maven by running the following
command.  Replace "com.example", "mydtapp" and "1.0-SNAPSHOT" with the
appropriate values (make sure this is all on one line):

```
 $ mvn archetype:generate
 -DarchetypeRepository=https://www.datatorrent.com/maven/content/reposito
 ries/releases
 -DarchetypeGroupId=com.datatorrent
 -DarchetypeArtifactId=apex-app-archetype -DarchetypeVersion=3.2.0
 -DgroupId=com.example -Dpackage=com.example.mydtapp -DartifactId=mydtapp
 -Dversion=1.0-SNAPSHOT
```

This creates a Maven project named "mydtapp". Open it with your favorite
IDE (e.g. NetBeans, Eclipse, IntelliJ IDEA). In the project, there is a
sample DAG that generates a number of tuples with a random number and
prints out "hello world" and the random number in the tuples.  The code
that builds the DAG is in
src/main/java/com/example/mydtapp/Application.java, and the code that
runs the unit test for the DAG is in
src/test/java/com/example/mydtapp/ApplicationTest.java. Try it out by
running the following command:

```
 $cd mydtapp; mvn package
```
This builds the App Package runs the unit test of the DAG.  You should
be getting test output similar to this:

```
 -------------------------------------------------------
  TESTS
 -------------------------------------------------------

 Running com.example.mydtapp.ApplicationTest
 hello world: 0.8015370953286478
 hello world: 0.9785359225545481
 hello world: 0.6322611586644047
 hello world: 0.8460953663451775
 hello world: 0.5719372906929072
 hello world: 0.6361174312337172
 hello world: 0.14873007534816318
 hello world: 0.8866986277418261
 hello world: 0.6346526809866057
 hello world: 0.48587295703904465
 hello world: 0.6436832429676687

 ...

 Tests run: 1, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 11.863
 sec

 Results :

 Tests run: 1, Failures: 0, Errors: 0, Skipped: 0
```

The "mvn package" command creates the App Package file in target
directory as target/mydtapp-1.0-SNAPSHOT.apa. You will be able to use
that App Package file to launch this sample application in your actual
Apex installation.

## Using IDE

Alternatively, you can do the above steps all within your IDE.  For
example, in NetBeans, select File -\> New Project.  Then choose “Maven”
and “Project from Archetype” in the dialog box, as shown.

![](images/AppPackage/ApplicationPackages.html-image00.png)

Then fill the Group ID, Artifact ID, Version and Repository entries as shown below.

![](images/AppPackage/ApplicationPackages.html-image02.png)

Group ID: com.datatorrent
Artifact ID: apex-app-archetype
Version: 3.0.0 (or any later version)

Repository:
[https://www.datatorrent.com/maven/content/repositories/releases](https://www.datatorrent.com/maven/content/repositories/releases)

Press Next and fill out the rest of the required information. For
example:

![](images/AppPackage/ApplicationPackages.html-image01.png)

Click Finish, and now you have created your own DataTorrent App Package
project, with a default unit test.  You can run the unit test, make code
changes or make dependency changes within your IDE.  The procedure for
other IDEs, like Eclipse or IntelliJ, is similar.

# 4. Writing Your Own App Package


Please refer to the [Application Developer Guide](https://www.datatorrent.com/docs/guides/ApplicationDeveloperGuide.html) on the basics on how to write a DataTorrent application.  In your AppPackage project, you can add custom operators (refer to [Operator Developer Guide](https://www.datatorrent.com/docs/guides/OperatorDeveloperGuide.html)), project dependencies, default and required configuration properties, pre-set configurations and other metadata.

## Adding (and removing) project dependencies

Under the project, you can add project dependencies in pom.xml, or do it
through your IDE.  Here’s the section that describes the dependencies in
the default pom.xml:
```
  <dependencies>
    <!-- add your dependencies here -->
    <dependency>
      <groupId>com.datatorrent</groupId>
      <artifactId>malhar-library</artifactId>
      <version>${datatorrent.version}</version>
      <!--
           If you know your application do not need the transitive dependencies that are pulled in by malhar-library,
           Uncomment the following to reduce the size of your app package.
      -->
      <!--
      <exclusions>
        <exclusion>
          <groupId>*</groupId>
          <artifactId>*</artifactId>
        </exclusion>
      </exclusions>
      -->
    </dependency>
    <dependency>
      <groupId>com.datatorrent</groupId>
      <artifactId>dt-engine</artifactId>
      <version>${datatorrent.version}</version>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>junit</groupId>
      <artifactId>junit</artifactId>
      <version>4.10</version>
      <scope>test</scope>
    </dependency>
  </dependencies>
```

By default, as shown above, the default dependencies include
malhar-library in compile scope, dt-engine in provided scope, and junit
in test scope.  Do not remove these three dependencies since they are
necessary for any Apex application.  You can, however, exclude
transitive dependencies from malhar-library to reduce the size of your
App Package, provided that none of the operators in malhar-library that
need the transitive dependencies will be used in your application.

In the sample application, it is safe to remove the transitive
dependencies from malhar-library, by uncommenting the "exclusions"
section.  It will reduce the size of the sample App Package from 8MB to
700KB.

Note that if we exclude \*, in some versions of Maven, you may get
warnings similar to the following:

```

 [WARNING] 'dependencies.dependency.exclusions.exclusion.groupId' for
 com.datatorrent:malhar-library:jar with value '*' does not match a
 valid id pattern.

 [WARNING]
 [WARNING] It is highly recommended to fix these problems because they
 threaten the stability of your build.
 [WARNING]
 [WARNING] For this reason, future Maven versions might no longer support
 building such malformed projects.
 [WARNING]

```
This is a bug in early versions of Maven 3.  The dependency exclusion is
still valid and it is safe to ignore these warnings.

## Application Configuration

A configuration file can be used to configure an application.  Different
kinds of configuration parameters can be specified. They are application
attributes, operator attributes and properties, port attributes, stream
properties and application specific properties. They are all specified
as name value pairs, in XML format, like the following.

```
<?xml version="1.0"?>
<configuration>
  <property>
    <name>some_name_1</name>
    <value>some_default_value</value>
  </property>
  <property>
    <name>some_name_2</name>
    <value>some_default_value</value>
  </property>
</configuration>
```

## Application attributes

Application attributes are used to specify the platform behavior for the
application. They can be specified using the parameter
```dt.attr.<attribute>```. The prefix “dt” is a constant, “attr” is a
constant denoting an attribute is being specified and ```<attribute>```
specifies the name of the attribute. Below is an example snippet setting
the streaming windows size of the application to be 1000 milliseconds.

```
  <property>
     <name>dt.attr.STREAMING_WINDOW_SIZE_MILLIS</name>
     <value>1000</value>
  </property>
```

The name tag specifies the attribute and value tag specifies the
attribute value. The name of the attribute is a JAVA constant name
identifying the attribute. The constants are defined in
com.datatorrent.api.Context.DAGContext and the different attributes can
be specified in the format described above.

## Operator attributes

Operator attributes are used to specify the platform behavior for the
operator. They can be specified using the parameter
```dt.operator.<operator-name>.attr.<attribute>```. The prefix “dt” is a
constant, “operator” is a constant denoting that an operator is being
specified, ```<operator-name>``` denotes the name of the operator, “attr” is
the constant denoting that an attribute is being specified and
```<attribute>``` is the name of the attribute. The operator name is the
same name that is specified when the operator is added to the DAG using
the addOperator method. An example illustrating the specification is
shown below. It specifies the number of streaming windows for one
application window of an operator named “input” to be 10

```
<property>
  <name>dt.operator.input.attr.APPLICATION_WINDOW_COUNT</name>
  <value>10</value>
</property>
```

The name tag specifies the attribute and value tag specifies the
attribute value. The name of the attribute is a JAVA constant name
identifying the attribute. The constants are defined in
com.datatorrent.api.Context.OperatorContext and the different attributes
can be specified in the format described above.

## Operator properties

Operators can be configured using operator specific properties. The
properties can be specified using the parameter
```dt.operator.<operator-name>.prop.<property-name>```. The difference
between this and the operator attribute specification described above is
that the keyword “prop” is used to denote that it is a property and
```<property-name>``` specifies the property name.  An example illustrating
this is specified below. It specifies the property “hostname” of the
redis server for a “redis” output operator.

```
  <property>
    <name>dt.operator.redis.prop.host</name>
    <value>127.0.0.1</value>
  </property>
```

The name tag specifies the property and the value specifies the property
value. The property name is converted to a setter method which is called
on the actual operator. The method name is composed by appending the
word “set” and the property name with the first character of the name
capitalized. In the above example the setter method would become
setHost. The method is called using JAVA reflection and the property
value is passed as an argument. In the above example the method setHost
will be called on the “redis” operator with “127.0.0.1” as the argument.

## Port attributes 
Port attributes are used to specify the platform behavior for input and
output ports. They can be specified using the parameter ```dt.operator.<operator-name>.inputport.<port-name>.attr.<attribute>```
for input port and ```dt.operator.<operator-name>.outputport.<port-name>.attr.<attribute>```
for output port. The keyword “inputport” is used to denote an input port
and “outputport” to denote an output port. The rest of the specification
follows the conventions described in other specifications above. An
example illustrating this is specified below. It specifies the queue
capacity for an input port named “input” of an operator named “range” to
be 4k.

```
<property>
  <name>dt.operator.range.inputport.input.attr.QUEUE_CAPACITY</name>
  <value>4000</value>
</property>
```

The name tag specifies the attribute and value tag specifies the
attribute value. The name of the attribute is a JAVA constant name
identifying the attribute. The constants are defined in
com.datatorrent.api.Context.PortContext and the different attributes can
be specified in the format described above.

The attributes for an output port can also be specified in a similar way
as described above with a change that keyword “outputport” is used
instead of “intputport”. A generic keyword “port” can be used to specify
either an input or an output port. It is useful in the wildcard
specification described below.

## Stream properties

Streams can be configured using stream properties. The properties can be
specified using the parameter
```dt.stream.<stream-name>.prop.<property-name>```  The constant “stream”
specifies that it is a stream, ```<stream-name>``` specifies the name of the
stream and ```<property-name>``` the name of the property. The name of the
stream is the same name that is passed when the stream is added to the
DAG using the addStream method. An example illustrating the
specification is shown below. It sets the locality of the stream named
“stream1” to container local indicating that the operators the stream is
connecting be run in the same container.

```
  <property>
    <name>dt.stream.stream1.prop.locality</name>
    <value>CONTAINER_LOCAL</value>
  </property>
```

The property name is converted into a set method on the stream in the
same way as described in operator properties section above. In this case
the method would be setLocality and it will be called in the stream
“stream1” with the value as the argument.

Along with the above system defined parameters, the applications can
define their own specific parameters they can be specified in the
configuration file. The only condition is that the names of these
parameters don’t conflict with the system defined parameters or similar
application parameters defined by other applications. To this end, it is
recommended that the application parameters have the format
```<full-application-class-name>.<param-name>.``` The
full-application-class-name is the full JAVA class name of the
application including the package path and param-name is the name of the
parameter within the application. The application will still have to
still read the parameter in using the configuration API of the
configuration object that is passed in populateDAG.

##  Wildcards

Wildcards and regular expressions can be used in place of names to
specify a group for applications, operators, ports or streams. For
example, to specify an attribute for all ports of an operator it can be
done as follows
```
<property>
  <name>dt.operator.range.port.*.attr.QUEUE_CAPACITY</name>
  <value>4000</value>
</property>
```

The wildcard “\*” was used instead of the name of the port. Wildcard can
also be used for operator name, stream name or application name. Regular
expressions can also be used for names to specify attributes or
properties for a specific set.

##Adding configuration properties

It is common for applications to require configuration parameters to
run.  For example, the address and port of the database, the location of
a file for ingestion, etc.  You can specify them in
src/main/resources/META-INF/properties.xml under the App Package
project. The properties.xml may look like:

```
<?xml version="1.0"?>
<configuration>
  <property>
    <name>some_name_1</name>
  </property>
  <property>
    <name>some_name_2</name>
    <value>some_default_value</value>
  </property>
</configuration>
```

The name of an application-specific property takes the form of:

```dt.operator.{opName}.prop.{propName} ```

The first represents the property with name propName of operator opName.
 Or you can set the application name at run time by setting this
property:

        dt.attr.APPLICATION_NAME

There are also other properties that can be set.  For details on
properties, refer to the [Operation and Installation Guide](https://www.datatorrent.com/docs/guides/OperationandInstallationGuide.html).

In this example, property some_name_1 is a required property which
must be set at launch time, or it must be set by a pre-set configuration
(see next section).  Property some\_name\_2 is a property that is
assigned with value some\_default\_value unless it is overridden at
launch time.

##Adding pre-set configurations


At build time, you can add pre-set configurations to the App Package by
adding configuration XML files under ```src/site/conf/<conf>.xml```in your
project.  You can then specify which configuration to use at launch
time.  The configuration XML is of the same format of the properties.xml
file.

##Application-specific properties file

You can also specify properties.xml per application in the application
package.  Just create a file with the name properties-{appName}.xml and
it will be picked up when you launch the application with the specified
name within the application package.  In short:

  properties.xml: Properties that are global to the Configuration
Package

  properties-{appName}.xml: Properties that are specific when launching
an application with the specified appName.

## Properties source precedence

If properties with the same key appear in multiple sources (e.g. from
app package default configuration as META-INF/properties.xml, from app
package configuration in the conf directory, from launch time defines,
etc), the precedence of sources, from highest to lowest, is as follows:

1. Launch time defines (using -D option in CLI, or the POST payload
    with the Gateway REST API’s launch call)
2. Launch time specified configuration file in file system (using -conf
    option in CLI)
3. Launch time specified package configuration (using -apconf option in
    CLI or the conf={confname} with Gateway REST API’s launch call)
4. Configuration from \$HOME/.dt/dt-site.xml
5. Application defaults within the package as
    META-INF/properties-{appname}.xml
6. Package defaults as META-INF/properties.xml
7. dt-site.xml in local DT installation
8. dt-site.xml stored in HDFS

## Other meta-data

In a Apex App Package project, the pom.xml file contains a
section that looks like:

```
<properties>
  <datatorrent.version>3.0.0</datatorrent.version>
  <datatorrent.apppackage.classpath\>lib*.jar</datatorrent.apppackage.classpath>
</properties>
```
datatorrent.version is the DataTorrent RTS version that are to be used
with this Application Package.

datatorrent.apppackage.classpath is the classpath that is used when
launching the application in the Application Package.  The default is
lib/\*.jar, where lib is where all the dependency jars are kept within
the Application Package.  One reason to change this field is when your
Application Package needs the classpath in a specific order.

## Logging configuration

Just like other Java projects, you can change the logging configuration
by having your log4j.properties under src/main/resources.  For example,
if you have the following in src/main/resources/log4j.properties:
```
 log4j.rootLogger=WARN,CONSOLE
 log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender
 log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout
 log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} [%t] %-5p
 %c{2} %M - %m%n
```

The root logger’s level is set to WARN and the output is set to the console (stdout).

Note that by default from project created from the maven archetype,
there is already a log4j.properties file under src/test/resources and
that file is only used for the unit test.

#5. Zip Structure of Application Package


DataTorrent Application Package files are zip files.  You can examine the content of any Application Package by using unzip -t on your Linux command line.

There are four top level directories in an Application Package:

1. "app" contains the jar files of the DAG code and any custom operators.
2. "lib" contains all dependency jars
3. "conf" contains all the pre-set configuration XML files.
4. "META-INF" contains the MANIFEST.MF file and the properties.xml file.
5. “resources” contains other files that are to be served by the Gateway on behalf of the app package.


#6. Managing Application Packages Through DT Gateway

The DT Gateway provides storing and retrieving Application Packages to
and from your distributed file system, e.g. HDFS.

##Storing an Application Package

You can store your Application Packages through DT Gateway using this
REST call:

```
 POST /ws/v2/appPackages
```

The payload is the raw content of your Application Package.  For
example, you can issue this request using curl on your Linux command
line like this, assuming your DT Gateway is accepting requests at
localhost:9090:

```
$ curl -XPOST -T <app-package-file> http://localhost:9090/ws/v2/appPackages
```

##Getting Meta Information on Application Packages


You can get the meta information on Application Packages stored through
DT Gateway using this call.  The information includes the logical plan
of each application within the Application Package.

```
 GET /ws/v2/appPackages/{owner}/{pkgName}/{pkgVersion}
```

## Getting Available Operators In Application Package

You can get the list of available operators in the Application Package
using this call.

```
GET /ws/v2/appPackages/{owner}/{pkgName}/{pkgVersion}/operators?parent={parent}
```

The parent parameter is optional.  If given, parent should be the fully
qualified class name.  It will only return operators that derive from
that class or interface. For example, if parent is
com.datatorrent.api.InputOperator, this call will only return input
operators provided by the Application Package.

## Getting Properties of Operators in Application Package

You can get the list of properties of any operator in the Application
Package using this call.

```
GET  /ws/v2/appPackages/{owner}/{pkgName}/{pkgVersion}/operators/{className}
```

## Getting List of Pre-Set Configurations in Application Package

You can get a list of pre-set configurations within the Application
Package using this call.

```
GET /ws/v2/appPackages/{owner}/{pkgName}/{packageVersion}/configs
```

You can also get the content of a specific pre-set configuration within
the Application Package.

```
 GET /ws/v2/appPackages/{owner}/{pkgName}/{pkgVersion}/configs/{configName}
```

## Changing Pre-Set Configurations in Application Package

You can create or replace pre-set configurations within the Application
Package
```
 PUT   /ws/v2/appPackages/{owner}/{pkgName}/{pkgVersion}/configs/{configName}
```
The payload of this PUT call is the XML file that represents the pre-set configuration.  The Content-Type of the payload is "application/xml" and you can delete a pre-set configuration within the Application Package.
```
 DELETE /ws/v2/appPackages/{owner}/{pkgName}/{pkgVersion}/configs/{configName}
```

## Retrieving an Application Package

You can download the Application Package file.  This Application Package
is not necessarily the same file as the one that was originally uploaded
since the pre-set configurations may have been modified.

```
 GET /ws/v2/appPackages/{owner}/{pkgName}/{pkgVersion}/download
```

## Launching an Application Package

You can launch an application within an Application Package.
```
POST /ws/v2/appPackages/{owner}/{pkgName}/{pkgVersion}/applications/{appName}/launch?config={configName}
```

The config parameter is optional.  If given, it must be one of the
pre-set configuration within the given Application Package.  The
Content-Type of the payload of the POST request is "application/json"
and should contain the properties to be launched with the application.
 It is of the form:

```
 {"property-name":"property-value", ... }
```

Here is an example of launching an application through curl:

```
 $ curl -XPOST -d'{"dt.operator.console.prop.stringFormat":"xyz %s"}'
 http://localhost:9090/ws/v2/appPackages/dtadmin/mydtapp/1.0-SNAPSHOT/app
 lications/MyFirstApplication/launch
```

Please refer to the [Gateway API reference](https://www.google.com/url?q=https://www.datatorrent.com/docs/guides/DTGatewayAPISpecification.html&sa=D&usg=AFQjCNEWfN7-e7fd6MoWZjmJUE3GW7UwdQ) for the complete specification of the REST API.

# 7. Examining and Launching Application Packages Through Apex CLI

If you are working with Application Packages in the local filesystem and
do not want to deal with dtGateway, you can use the Apex
Command Line Interface (dtcli).  Please refer to the [Gateway API reference](https://www.datatorrent.com/docs/guides/DTGatewayAPISpecification.html)
to see samples for these commands.

##Getting Application Package Meta Information

You can get the meta information about the Application Package using
this Apex CLI command.

```
 dt> get-app-package-info <app-package-file>
```

## Getting Available Operators In Application Package

You can get the list of available operators in the Application Package
using this command.

```
 dt> get-app-package-operators <app-package-file> <package-prefix>
 [parent-class]
```

## Getting Properties of Operators in Application Package

You can get the list of properties of any operator in the Application
Package using this command.

 dt> get-app-package-operator-properties <app-package-file> <operator-class>


##Launching an Application Package

You can launch an application within an Application Package.
```
dt> launch [-D property-name=property-value, ...] [-conf config-name]
 [-apconf config-file-within-app-package] <app-package-file>
 [matching-app-name] 
 ```
Note that -conf expects a configuration file in the file system, while -apconf expects a configuration file within the app package.

© 2014-2015 DataTorrent Inc.





