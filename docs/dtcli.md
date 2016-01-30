Apache Apex Command Line Interface
================================================================================

dtCli, the Apache Apex command line interface, can be used to launch, monitor, and manage
Apache Apex applications.  dtCli is a wrapper around the [REST API](dtgateway_api.md) provided by dtGatway, and
provides a developer friendly way of interacting with Apache Apex platform. The CLI enables a much higher level of feature set by
hiding deep details of REST API.  Another advantage of dtCli is to provide scope, by connecting and executing commands in a context
of specific application.  dtCli enables easy integration with existing enterprise toolset for automated application monitoring
and management.  Currently the following high level tasks are supported.

-   Launch or kill applications
-   View system metrics including load, throughput, latency, etc.
-   Start or stop tuple recording
-   Read operator, stream, port properties and attributes
-   Write to operator properties
-   Dynamically change the application logical plan
-   Create custom macros


## dtcli Commands

dtCli can be launched by running following command on the same machine where dtGatway was installed

    dtcli

Help on all commands is available via “help” command in the CLI

### Global Commands

```
GLOBAL COMMANDS EXCEPT WHEN CHANGING LOGICAL PLAN:

alias alias-name command
	Create a command alias

begin-macro name
	Begin Macro Definition ($1...$9 to access parameters and type 'end' to end the definition)

connect app-id
	Connect to an app

dump-properties-file out-file jar-file class-name
	Dump the properties file of an app class

echo [arg ...]
	Echo the arguments

exit
	Exit the CLI

get-app-info app-id
	Get the information of an app

get-app-package-info app-package-file
	Get info on the app package file

get-app-package-operator-properties app-package-file operator-class
	Get operator properties within the given app package

get-app-package-operators [options] app-package-file [search-term]
	Get operators within the given app package
	Options:
            -parent    Specify the parent class for the operators

get-config-parameter [parameter-name]
	Get the configuration parameter

get-jar-operator-classes [options] jar-files-comma-separated [search-term]
	List operators in a jar list
	Options:
            -parent    Specify the parent class for the operators

get-jar-operator-properties jar-files-comma-separated operator-class-name
	List properties in specified operator

help [command]
	Show help

kill-app app-id [app-id ...]
	Kill an app

  launch [options] jar-file/json-file/properties-file/app-package-file [matching-app-name]
  	Launch an app
  	Options:
            -apconf <app package configuration file>        Specify an application
                                                            configuration file
                                                            within the app
                                                            package if launching
                                                            an app package.
            -archives <comma separated list of archives>    Specify comma
                                                            separated archives
                                                            to be unarchived on
                                                            the compute machines.
            -conf <configuration file>                      Specify an
                                                            application
                                                            configuration file.
            -D <property=value>                             Use value for given
                                                            property.
            -exactMatch                                     Only consider
                                                            applications with
                                                            exact app name
            -files <comma separated list of files>          Specify comma
                                                            separated files to
                                                            be copied on the
                                                            compute machines.
            -ignorepom                                      Do not run maven to
                                                            find the dependency
            -libjars <comma separated list of libjars>      Specify comma
                                                            separated jar files
                                                            or other resource
                                                            files to include in
                                                            the classpath.
            -local                                          Run application in
                                                            local mode.
            -originalAppId <application id>                 Specify original
                                                            application
                                                            identifier for restart.
            -queue <queue name>                             Specify the queue to
                                                            launch the application

list-application-attributes
	Lists the application attributes
list-apps [pattern]
	List applications
list-operator-attributes
	Lists the operator attributes
list-port-attributes
	Lists the port attributes
set-pager on/off
	Set the pager program for output
show-logical-plan [options] jar-file/app-package-file [class-name]
	List apps in a jar or show logical plan of an app class
	Options:
            -exactMatch                                Only consider exact match
                                                       for app name
            -ignorepom                                 Do not run maven to find
                                                       the dependency
            -libjars <comma separated list of jars>    Specify comma separated
                                                       jar/resource files to
                                                       include in the classpath.
shutdown-app app-id [app-id ...]
	Shutdown an app
source file
	Execute the commands in a file
```

### Commands after connecting to an application

```
COMMANDS WHEN CONNECTED TO AN APP (via connect <appid>) EXCEPT WHEN CHANGING LOGICAL PLAN:

begin-logical-plan-change
	Begin Logical Plan Change
dump-properties-file out-file [jar-file] [class-name]
	Dump the properties file of an app class
get-app-attributes [attribute-name]
	Get attributes of the connected app
get-app-info [app-id]
	Get the information of an app
get-operator-attributes operator-name [attribute-name]
	Get attributes of an operator
get-operator-properties operator-name [property-name]
	Get properties of a logical operator
get-physical-operator-properties [options] operator-id
	Get properties of a physical operator
	Options:
            -propertyName <property name>    The name of the property whose
                                             value needs to be retrieved
            -waitTime <wait time>            How long to wait to get the result
get-port-attributes operator-name port-name [attribute-name]
	Get attributes of a port
get-recording-info [operator-id] [start-time]
	Get tuple recording info
kill-app [app-id ...]
	Kill an app
kill-container container-id [container-id ...]
	Kill a container
list-containers
	List containers
list-operators [pattern]
	List operators
set-operator-property operator-name property-name property-value
	Set a property of an operator
set-physical-operator-property operator-id property-name property-value
	Set a property of an operator
show-logical-plan [options] [jar-file/app-package-file] [class-name]
	Show logical plan of an app class
	Options:
            -exactMatch                                Only consider exact match
                                                       for app name
            -ignorepom                                 Do not run maven to find
                                                       the dependency
            -libjars <comma separated list of jars>    Specify comma separated
                                                       jar/resource files to
                                                       include in the classpath.
show-physical-plan
	Show physical plan
shutdown-app [app-id ...]
	Shutdown an app
start-recording operator-id [port-name] [num-windows]
	Start recording
stop-recording operator-id [port-name]
	Stop recording
wait timeout
	Wait for completion of current application
```

### Commands when changing the logical plan

```
COMMANDS WHEN CHANGING LOGICAL PLAN (via begin-logical-plan-change):

abort
	Abort the plan change
add-stream-sink stream-name to-operator-name to-port-name
	Add a sink to an existing stream
create-operator operator-name class-name
	Create an operator
create-stream stream-name from-operator-name from-port-name to-operator-name to-port-name
	Create a stream
help [command]
	Show help
remove-operator operator-name
	Remove an operator
remove-stream stream-name
	Remove a stream
set-operator-attribute operator-name attr-name attr-value
	Set an attribute of an operator
set-operator-property operator-name property-name property-value
	Set a property of an operator
set-port-attribute operator-name port-name attr-name attr-value
	Set an attribute of a port
set-stream-attribute stream-name attr-name attr-value
	Set an attribute of a stream
show-queue
	Show the queue of the plan change
submit
	Submit the plan change
```



## Examples

An example of defining a custom macro.  The macro updates a running application by inserting a new operator.  It takes three parameters and executes a logical plan changes.

```
dt> begin-macro add-console-output
macro> begin-logical-plan-change
macro> create-operator $1 com.datatorrent.lib.io.ConsoleOutputOperator
macro> create-stream stream_$1 $2 $3 $1 in
macro> submit
```


Then execute the `add-console-output` macro like this

```
dt> add-console-output xyz opername portname
```

This macro then expands to run the following command

```
begin-logical-plan-change
create-operator xyz com.datatorrent.lib.io.ConsoleOutputOperator
create-stream stream_xyz opername portname xyz in
submit
```


*Note*:  To perform runtime logical plan changes, like ability to add new operators,
they must be part of the jar files that were deployed at application launch time.
