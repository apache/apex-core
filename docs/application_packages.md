Apache Apex Packages
==================================================

# Application Packages

An Apache Apex Application Package is a zip file that contains all the
necessary files to launch an application in Apache Apex. It is the
standard way for assembling and sharing an Apache Apex application.

## Requirements

You will need have the following installed:

1. Apache Maven 3.0 or later (for assembling the App Package)
2. Apache Apex 3.6.0 or later (for launching the App Package in your cluster)

## Creating Your First Apex App Package

You can create an Apex Application Package using your Linux command
line, or using your favorite IDE.

### Using Command Line

First, change to the directory where you put your projects, and create
an Apex application project using Maven by running the following
command.  Replace "com.example", "myapp" and "1.0-SNAPSHOT" with the
appropriate values (make sure this is all on one line). You can also
replace "RELEASE" with a specific Apex version number (like "3.6.0")
if you don't want to use the most recent release:

    $ mvn archetype:generate \
     -DarchetypeGroupId=org.apache.apex \
     -DarchetypeArtifactId=apex-app-archetype -DarchetypeVersion=RELEASE \
     -DgroupId=com.example -Dpackage=com.example.myapp -DartifactId=myapp \
     -Dversion=1.0-SNAPSHOT

This creates a Maven project named "myapp". Open it with your favorite
IDE (e.g. NetBeans, Eclipse, IntelliJ IDEA). In the project, there is a
sample DAG that generates a number of tuples with a random number and
prints out "hello world" and the random number in the tuples.  The code
that builds the DAG is in
src/main/java/com/example/myapp/Application.java, and the code that
runs the unit test for the DAG is in
src/test/java/com/example/myapp/ApplicationTest.java. Try it out by
running the following command:

    $cd myapp; mvn package

This builds the App Package runs the unit test of the DAG.  You should
be getting test output similar to this:

```
 -------------------------------------------------------
  TESTS
 -------------------------------------------------------

 Running com.example.myapp.ApplicationTest
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
directory as target/myapp-1.0-SNAPSHOT.apa. You will be able to use
that App Package file to launch this sample application in your actual
Apex installation.

Alternatively you can perform the same steps within your IDE (IDEA IntelliJ, Eclipse, NetBeans all support it). Please check the IDE documentation for details.

Group ID: org.apache.apex
Artifact ID: apex-app-archetype
Version: 3.6.0 (or any later version)

## Writing Your Own App Package


Please refer to the [Application Developer Guide][application_development.md] on the basics on how to write an Apache Apex application. In your AppPackage project, you can add custom operators (refer to [Operator Development Guide](operator_development.md), project dependencies, default and required configuration properties, pre-set configurations and other metadata. Note that you can also specify the DAG using Java, JSON or properties files. 

### Adding (and removing) project dependencies

Under the project, you can add project dependencies in pom.xml, or do it
through your IDE.  Here’s the section that describes the dependencies in
the default pom.xml:
```xml
  <dependencies>
    <!-- add your dependencies here -->
    <dependency>
      <groupId>org.apache.apex</groupId>
      <artifactId>malhar-library</artifactId>
      <version>${apex.version}</version>
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
      <groupId>org.apache.apex</groupId>
      <artifactId>apex-engine</artifactId>
      <version>${apex.version}</version>
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
malhar-library in compile scope, apex-engine in provided scope, and junit
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
 org.apache.apex:malhar-library:jar with value '*' does not match a
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

### Application Configuration

A configuration file can be used to configure an application.  Different
kinds of configuration parameters can be specified. They are application
attributes, operator attributes and properties, port attributes, stream
properties and application specific properties. They are all specified
as name value pairs, in XML format, like the following.

```xml
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

### Application attributes

Application attributes are used to specify the platform behavior for the
application. They can be specified using the parameter
```apex.attr.<attribute>```. The prefix “apex” is a constant, “attr” is a
constant denoting an attribute is being specified and ```<attribute>```
specifies the name of the attribute. Below is an example snippet setting
the streaming windows size of the application to be 1000 milliseconds.

```xml
  <property>
     <name>apex.attr.STREAMING_WINDOW_SIZE_MILLIS</name>
     <value>1000</value>
  </property>
```

The name tag specifies the attribute and value tag specifies the
attribute value. The name of the attribute is a JAVA constant name
identifying the attribute. The constants are defined in
com.datatorrent.api.Context.DAGContext and the different attributes can
be specified in the format described above.

### Operator attributes

Operator attributes are used to specify the platform behavior for the
operator. They can be specified using the parameter
```apex.operator.<operator-name>.attr.<attribute>```. The prefix “apex” is a
constant, “operator” is a constant denoting that an operator is being
specified, ```<operator-name>``` denotes the name of the operator, “attr” is
the constant denoting that an attribute is being specified and
```<attribute>``` is the name of the attribute. The operator name is the
same name that is specified when the operator is added to the DAG using
the addOperator method. An example illustrating the specification is
shown below. It specifies the number of streaming windows for one
application window of an operator named “input” to be 10

```xml
<property>
  <name>apex.operator.input.attr.APPLICATION_WINDOW_COUNT</name>
  <value>10</value>
</property>
```

The name tag specifies the attribute and value tag specifies the
attribute value. The name of the attribute is a JAVA constant name
identifying the attribute. The constants are defined in
com.datatorrent.api.Context.OperatorContext and the different attributes
can be specified in the format described above.

### Operator properties

Operators can be configured using operator specific properties. The
properties can be specified using the parameter
```apex.operator.<operator-name>.prop.<property-name>```. The difference
between this and the operator attribute specification described above is
that the keyword “prop” is used to denote that it is a property and
```<property-name>``` specifies the property name.  An example illustrating
this is specified below. It specifies the property “hostname” of the
redis server for a “redis” output operator.

```xml
  <property>
    <name>apex.operator.redis.prop.host</name>
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

Properties that are collection types can also be configured, based on the beanutils
syntax. For example, the connection properties of the JDBC store can be accessed
like this:

```xml
  <property>
    <name>apex.operator.jdbc.prop.store.connectionProperties(user)</name>
    <value>your-user-name</value>
  </property>
```

### Port attributes
Port attributes are used to specify the platform behavior for input and
output ports. They can be specified using the parameter ```apex.operator.<operator-name>.inputport.<port-name>.attr.<attribute>```
for input port and ```apex.operator.<operator-name>.outputport.<port-name>.attr.<attribute>```
for output port. The keyword “inputport” is used to denote an input port
and “outputport” to denote an output port. The rest of the specification
follows the conventions described in other specifications above. An
example illustrating this is specified below. It specifies the queue
capacity for an input port named “input” of an operator named “range” to
be 4k.

```xml
<property>
  <name>apex.operator.range.inputport.input.attr.QUEUE_CAPACITY</name>
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

### Stream properties

Streams can be configured using stream properties. The properties can be
specified using the parameter
```apex.stream.<stream-name>.prop.<property-name>```  The constant “stream”
specifies that it is a stream, ```<stream-name>``` specifies the name of the
stream and ```<property-name>``` the name of the property. The name of the
stream is the same name that is passed when the stream is added to the
DAG using the addStream method. An example illustrating the
specification is shown below. It sets the locality of the stream named
“stream1” to container local indicating that the operators the stream is
connecting be run in the same container.

```xml
  <property>
    <name>apex.stream.stream1.prop.locality</name>
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

###  Wildcards

Wildcards and regular expressions can be used in place of names to
specify a group for applications, operators, ports or streams. For
example, to specify an attribute for all ports of an operator it can be
done as follows
```xml
<property>
  <name>apex.operator.range.port.*.attr.QUEUE_CAPACITY</name>
  <value>4000</value>
</property>
```

The wildcard “\*” was used instead of the name of the port. Wildcard can
also be used for operator name, stream name or application name. Regular
expressions can also be used for names to specify attributes or
properties for a specific set.

### Adding configuration properties

It is common for applications to require configuration parameters to
run.  For example, the address and port of the database, the location of
a file for ingestion, etc.  You can specify them in
src/main/resources/META-INF/properties.xml under the App Package
project. The properties.xml may look like:

```xml
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

```apex.operator.{opName}.prop.{propName} ```

The first represents the property with name propName of operator opName.
 Or you can set the application name at run time by setting this
property:

        apex.attr.APPLICATION_NAME


In this example, property some_name_1 is a required property which
must be set at launch time, or it must be set by a pre-set configuration
(see next section).  Property some\_name\_2 is a property that is
assigned with value some\_default\_value unless it is overridden at
launch time.

### Adding pre-set configurations


At build time, you can add pre-set configurations to the App Package by
adding configuration XML files under ```src/site/conf/<conf>.xml```in your
project.  You can then specify which configuration to use at launch
time.  The configuration XML is of the same format of the properties.xml
file.

### Application-specific properties file

You can also specify properties.xml per application in the application
package.  Just create a file with the name properties-{appName}.xml and
it will be picked up when you launch the application with the specified
name within the application package.  In short:

  properties.xml: Properties that are global to the Configuration
Package

  properties-{appName}.xml: Properties that are specific when launching
an application with the specified appName.

### Properties source precedence

If properties with the same key appear in multiple sources (e.g. from
app package default configuration as META-INF/properties.xml, from app
package configuration in the conf directory, from launch time defines,
etc), the precedence of sources, from highest to lowest, is as follows:

1. Launch time defines (using -D option in CLI)
2. Launch time specified configuration file in file system (using -conf
    option in CLI)
3. Launch time specified package configuration (using -apconf option in
    CLI)
4. Configuration from \$HOME/.dt/dt-site.xml
5. Application defaults within the package as
    META-INF/properties-{appname}.xml
6. Package defaults as META-INF/properties.xml
7. dt-site.xml in local DT installation
8. dt-site.xml stored in HDFS

### Other meta-data

In a Apex App Package project, the pom.xml file contains a
section that looks like:

```xml
<properties>
  <apex.core.version>3.6.0</apex.core.version>
  <apex.apppackage.classpath\>lib*.jar</apex.apppackage.classpath>
</properties>
```
apex.version is the Apache Apex version that are to be used
with this Application Package.

apex.apppackage.classpath is the classpath that is used when
launching the application in the Application Package.  The default is
lib/\*.jar, where lib is where all the dependency jars are kept within
the Application Package.  One reason to change this field is when your
Application Package needs the classpath in a specific order.

### Logging configuration

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


## Zip Structure of Application Package


Apache Apex Application Package files are zip files.  You can examine the content of any Application Package by using unzip -t on your Linux command line.

There are four top level directories in an Application Package:

1. "app" contains the jar files of the DAG code and any custom operators, and any JSON or properties files that specify a DAG.
2. "lib" contains all dependency jars
3. "conf" contains all the pre-set configuration XML files.
4. "META-INF" contains the MANIFEST.MF file and the properties.xml file.
5. “resources” contains any other files


## Examining and Launching Application Packages Through CLI

If you are working with Application Packages in the local filesystem, you can use the Apex Command Line Interface (apex).

### Getting Application Package Meta Information

You can get the meta information about the Application Package using
this Apex CLI command.

```
 apex> get-app-package-info <app-package-file>
```

### Getting Available Operators In Application Package

You can get the list of available operators in the Application Package
using this command.

```
 apex> get-app-package-operators <app-package-file> <package-prefix>
 [parent-class]
```

### Getting Properties of Operators in Application Package

You can get the list of properties of any operator in the Application
Package using this command.

 apex> get-app-package-operator-properties <app-package-file> <operator-class>


### Launching an Application Package

You can launch an application within an Application Package.
```
apex> launch [-D property-name=property-value, ...] [-conf config-name]
 [-apconf config-file-within-app-package] <app-package-file>
 [matching-app-name]
```
Note that -conf expects a configuration file in the file system, while -apconf expects a configuration file within the app package.

# Configuration Packages

Sometimes just a configuration file is not enough for launching an application package. If a configuration requires
additional files to be packaged, you can use an Apex Configuration Package.

## Creating Configuration Packages

Creating Configuration Packages is similar to creating Application Packages. You can create a configuration 
package project using Maven by running the following command. Replace "com.example", "myconfig" and "1.0-SNAPSHOT" with the appropriate values:

```
$ mvn archetype:generate -DarchetypeGroupId=org.apache.apex \
  -DarchetypeArtifactId=apex-conf-archetype -DarchetypeVersion=RELEASE \
  -DgroupId=com.example -Dpackage=com.example.myconfig -DartifactId=myconfig \
  -Dversion=1.0-SNAPSHOT
```

And create the configuration package file by running:

```
$ mvn package
```

The "mvn package" command creates the Config Package file in target
directory as target/myconfig.apc. You will be able to use that
Configuration Package file to launch an Apache Apex application.

## Assembling your own configuration package

Inside the project created by the archetype, these are the files that
you should know about when assembling your own configuration package:

    ./pom.xml
    ./src/main/resources/classpath
    ./src/main/resources/files
    ./src/main/resources/META-INF/properties.xml
    ./src/main/resources/META-INF/properties-{appname}.xml

### pom.xml

Example:

```xml
  <groupId>com.example</groupId>
  <version>1.0.0</version>
  <artifactId>myconfig</artifactId>
  <packaging>jar</packaging>
  <!-- change these to the appropriate values -->
  <name>My Apex Application Configuration</name>
  <description>My Custom Application Configuration Description</description>
  <properties>
    <apex.apppackage.name>myapexapp</apex.apppackage.name>
    <apex.apppackage.minversion>1.0.0</apex.apppackage.minversion>
    <apex.apppackage.maxversion>1.9999.9999</apex.apppackage.maxversion>
    <apex.appconf.classpath>classpath/*</apex.appconf.classpath>
    <apex.appconf.files>files/*</apex.appconf.files>
  </properties>

```
In pom.xml, you can change the following keys to your desired values

* ```<groupId>```
* ```<version>```
* ```<artifactId>```
* ```<name> ```
* ```<description>```

You can also change the values of

* ```<apex.apppackage.name>```
* ```<apex.apppackage.minversion>```
* ```<apex.apppackage.maxversion>```

to reflect what Application Packages can be used with this configuration package.  Apex will use this information to check whether a
configuration package is compatible with the Application Package when you issue a launch command.

### ./src/main/resources/classpath

Place any file in this directory that you’d like to be copied to the
compute machines when launching an application and included in the
classpath of the application.  Example of such files are Java properties
files and jar files.

### ./src/main/resources/files

Place any file in this directory that you’d like to be copied to the
compute machines when launching an application but not included in the
classpath of the application.

### Properties XML file

A properties xml file consists of a set of key-value pairs.  The set of
key-value pairs specifies the configuration options the application
should be launched with.

Example:
```xml
<configuration>
  <property>
    <name>some-property-name</name>
    <value>some-property-value</value>
  </property>
   ...
</configuration>
```
Names of properties XML file:

*  **properties.xml:** Properties that are global to the Configuration
Package
*  **properties-{appName}.xml:** Properties that are specific when launching
an application with the specified appName within the Application
Package.

After you are done with the above, remember to do mvn package to
generate a new configuration package, which will be located in the
target directory in your project.

### Zip structure of configuration package 
Apex Application Configuration Package files are zip files.  You
can examine the content of any Application Configuration Package by
using unzip -t on your Linux command line.  The structure of the zip
file is as follow:

```
META-INF
  MANIFEST.MF
  properties.xml
  properties-{appname}.xml
classpath
  {classpath files}
files
  {files}
```

### Launching with CLI

`-conf` option of the launch command in CLI supports specifying configuration package in the local filesystem.  Example:

    apex\> launch myapp-1.0.0.apa -conf myconfig.apc

This command expects both the application package and the configuration package to be in the local file system.

