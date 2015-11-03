Appendix
===

Operators in Top N words application
---
This section describes the operators used for building the top N
words application. The operators, the implementing classes and a brief description of their
functionalities are described in this table.

<table>
<colgroup>
<col width="25%" />
<col width="35%" />
<col width="40%" />
</colgroup>
<tbody>
<tr class="odd">
<td align="left"><p><b>Operator</b></p></td>
<td align="left"><p><b>Implementing class</b></p></td>
<td align="left"><p><b>Description</b></p></td>
</tr>
<tr class="even">
<td align="left"><p>lineReader</p></td>
<td align="left"><p>LineReader</p></td>
<td align="left"><p>Reads lines from input files.</p></td>
</tr>
<tr class="odd">
<td align="left"><p>wordReader</p></td>
<td align="left"><p>WordReader</p></td>
<td align="left"><p>Splits a line into words.</p></td>
</tr>
<tr class="even">
<td align="left"><p>windowWordCount</p></td>
<td align="left"><p>WindowWordCount</p></td>
<td align="left"><p>Computes word frequencies for a single window.</p></td>
</tr>
<tr class="odd">
<td align="left"><p>fileWordCount</p></td>
<td align="left"><p>FileWordCount</p></td>
<td align="left"><p>Maintains per-file and global word frequencies.</p></td>
</tr>
<tr class="even">
<td align="left"><p>wcWriter</p></td>
<td align="left"><p>WcWriter</p></td>
<td align="left"><p>Writes top N words and their frequencies to output files.</p></td>
</tr>
<tr class="odd">
<td align="left"><p>console</p></td>
<td align="left"><p>ConsoleOutputOperator</p></td>
<td align="left"><p>Writes received tuples to console.</p></td>
</tr>
<tr class="even">
<td align="left"><p>snapshotServerFile</p></td>
<td align="left"><p>AppDataSnapshotServerMap</p></td>
<td align="left"><p>Caches the last data set for the current file, and returns it in response to queries.</p></td>
</tr>
<tr class="odd">
<td align="left"><p>snapshotServerGlobal</p></td>
<td align="left"><p>AppDataSnapshotServerMap</p></td>
<td align="left"><p>Caches the last global data set, and returns it in response to queries.</p></td>
</tr>
<tr class="even">
<td align="left"><p>QueryFile</p></td>
<td align="left"><p>PubSubWebSocketAppDataQuery</p></td>
<td align="left"><p>Receives queries for per-file data.</p></td>
</tr>
<tr class="odd">
<td align="left"><p>QueryGlobal</p></td>
<td align="left"><p>PubSubWebSocketAppDataQuery</p></td>
<td align="left"><p>Receives queries for global data.</p></td>
</tr>
<tr class="even">
<td align="left"><p>wsResultFile</p></td>
<td align="left"><p>PubSubWebSocketAppDataResult</p></td>
<td align="left"><p>Returns results for per-file queries.</p></td>
</tr>
<tr class="odd">
<td align="left"><p>wsResultGlobal</p></td>
<td align="left"><p>PubSubWebSocketAppDataResult</p></td>
<td align="left"><p>Returns results for global queries.</p></td>
</tr>
</tbody>
</table>

We now describe the process of wiring these operators together in the
`populateDAG()` method of the main application class
`ApplicationWithQuerySupport`. First, the operators are created and added to
the DAG via the `addOperator` method:
```
LineReader lineReader = dag.addOperator("lineReader",new LineReader());
```

The first argument is a string that names this instance of the
operator; it is the same as the value in the first column of the above
table and also the node name in the Logical DAG.

Next, we connect each output port of an operator with all the input ports that
should receive these tuples using the `addStream` function, for example:

```
dag.addStream("lines", lineReader.output, wordReader.input);
...
dag.addStream("WordCountsFile", fileWordCount.outputPerFile, snapshotServerFile.input, console.input);
```

Notice that the stream from `fileWordCount.outputPerFile` (which consists of
the top N words for the current file as the file is being read) goes to
`snapshotServerFile.input` (where it will be saved to respond to queries) and to
`console.input` (which is used for debugging). Additional sinks can be provided
in the same call as additional terminal arguments. You can examine the rest of
these calls and ensure that they match the names and connections of the
Logical DAG.

This section provides detailed information about each operator.

**LineReader**

This class extends `AbstractFileInputOperator<String>` to open a file, read its
lines, and emit them as tuples. It has two output ports, one for the normal
output of tuples, and the other for the output of an EOF tuple indicating the
end of the current input file. Ports should always be transient fields because
they should not be serialized and saved to the disk during checkpointing.

The base class keeps track of files already processed, files that
should be ignored, and files that failed part-way. Derived classes need to
override four methods: `openFile`, `closeFile`, `readEntity`, and `emit`. Of
these, only the third is non-trivial: if a valid line is available, it is read
and returned. Otherwise, the end of the file must have been reached. To
indicate this, the file name is emitted on the control port where it
will be read by the `FileWordCount` operator.

**WordReader**

This operator receives lines from `LineReader` on the input port and emits
words on the output port. It has a configurable property called `nonWordStr`
along with associated public getter and setter methods. Such properties can be
customized in the appropriate properties file of the application. The values of
the properties are automatically injected into the operator at run-time. In
this scenario, this string is provided the value of the property
`dt.application.TopNWordsWithQueries.operator.wordReader.nonWordStr`.
For efficiency, this string is compiled into a pattern for repeated use.
The `process` method of the input port splits each input line into words using
this pattern as the separator, and emits non-empty words on the output port.

**WindowWordCount**

This operator receives words and emits a list of word-frequency pairs for each
window. It maintains a word-frequency map for the current window, updates this
map for each word received, emits the whole map (if non-empty) when
`endWindow` is called, and clears the map in preparation for the next window.
This design pattern is appropriate because for normal text files, the number of
words received is far more than the size of the accumulated map. However, for
situations where data is emitted for each tuple, you should not wait till the
`endWindow` call, but rather emit output tuples as each input tuple is
processed.

**FileWordCount**

This operator has two input ports, one for the per-window frequency maps it
gets from the previous operator, and a control port to receive the file name
when `LineReader` reaches the end of a file. When a file name is received on
the control port, it is saved and the final results for the file appear as
output at the next `endWindow`. The reason for waiting is subtle: there is no
guarantee of the relative order in which tuples arrive at two input ports;
additional input tuples from the same window can arrive at the input port
even after the EOF was received on the control port. Note however that we _do_
have a guarantee that tuples on the input port will arrive in exactly the same
order in which they were emitted on the output port between the bracketing
`beginWindow` and `endWindow` calls by the upstream operator.

This operator also has three output ports: the `outputPerFile` port for the top
N pairs for the current file as it is being read; the `outputGlobal` port for
the global top N pairs, and the `fileOutput` port for the final top N pairs for
the current file computed after receiving the EOF control tuple. The output
from the first is sent to the per-file snapshot server, the output from
the second is sent to the global snapshot server, and the output from the last
is sent to the operator that writes results to the output file.

_FileWordCount_ also maintains two maps for per-file and global frequency
counts because they track frequencies of all words seen so far. These maps
can get very large as more and more files are processed.

_FileWordCount_ has a configurable property `topN` for the number of top pairs we
are interested in. This was configured in our properties file with a value of
10 and the property name: `dt.application.TopNWordsWithQueries.operator.fileWordCount.topN`

In the `endWindow` call, both maps are passed to the `getTopNList` function
where they are flattened, sorted in descending order of frequency, stripped of
all but the top N pairs, and returned for output. There are a couple of
additional fields used to cast the output into the somewhat peculiar form
required by the snapshot server.

**WordCountWriter**

This operator extends `AbstractFileOutputOperator<Map<String,Object>>`, and
simply writes the final top N pairs to the output file. As with _LineReader_,
most of the complexity of _WordCountWriter_ is hidden in the base class. You must
provide implementations for 3 methods: `endWindow`, `getFileName`, and
`getBytesForTuple`. The first method calls the base class method `requestFinalize`.
The output file is written periodically to temporary files
with a synthetic file name that includes a timestamp. These files are removed
and the actual desired file name is restored by this call. The `getFileName`
method retrieves the file name from the tuple, and the `getBytesForTuple`
method converts the list of pairs to a string in the desired format.

**ConsoleOutputOperator**

This is an output operator that is a part of the Malhar library. It simply
writes incoming tuples to the console and is useful when debugging.

**AppDataSnapshotServerMap**

This operator is also part of the Malhar library and is used to store snapshots
of data. These snapshots are used to respond to queries. For this application,
we use two snapshots &mdash;  one for a per-file top N snapshot and one for a
global snapshot.

**PubSubWebSocketAppDataQuery**

This is an input operator that is a part of the Malhar library. It is used to
send queries to an operator via the Data Torrent Gateway, which can act as a
message broker for limited amounts of data using a topic-based
publish-subscribe model. The URL to connect is typically something like:
```
ws://gateway-host:port/pubsub
```
where _gateway-host_ and _port_ should be replaced by appropriate values.

A publisher sends a JSON message to the URL where the value of the `data` key
is the desired message content. The JSON might look like this:
```
{"type":"publish", "topic":"foobar", "data": ...}
```
Correspondingly, subscribers send messages like this to retrieve published
message data:
```
{"type":"subscribe", "topic":"foobar"}
```
Topic names need not be pre-registered anywhere but the same topic
name (for example, _foobar_ in the example) must be used by both publisher and
subscriber. Additionally, if there are no subscribers when a message is
published, it is simply discarded.

For this tutorial, two query operators are used: one for per-file queries and
one for global queries. The topic names were configured in the properties file
earlier with values `TopNWordsQueryFile` and `TopNWordsQueryGlobal` under the
respective names:
```
dt.application.TopNWordsWithQueries.operator.QueryFile.topic
dt.application.TopNWordsWithQueries.operator.QueryGlobal.topic
```

**PubSubWebSocketAppDataResult**

Analogous to the previous operator, this is an output operator used to publish
query results to a gateway topic. You must use two of these to match the query
operators, and configure their topics in the properties file with values
`TopNWordsQueryFileResult` and `TopNWordsQueryGlobalResult` corresponding to
the respective names:
```
dt.application.TopNWordsWithQueries.operator.wsResultFile.topic
dt.application.TopNWordsWithQueries.operator.wsResultGlobal.topic
```

Further Exploration
---
In this tutorial, the property values in the `properties.xml` file were set to
limit the amount of memory allocated to each operator. You can try varying
these values and checking the impact of such an operation on the stability and
performance of the application. You can also explore the largest text
file that the application can handle.

Another aspect to explore is fixing the current limitation of
one-file-at-a-time processing; if multiple files are dropped into the
input directory simultaneously, the file reader can switch from one file to the
next in the same window. When the `FileWordCount` operator gets an EOF on the
control port, it waits for an `endWindow` call to emit word counts so those
counts will be incorrect if tuples from two different files arrive in the same
window. Try fixing this issue.

DataTorrent terminology
---
**Operators**

Operators are basic computation units that have properties and
attributes, and are interconnected via streams to form an application.
Properties customize the functional definition of the operator, while
attributes customize the operational behavior. You can think of
operators as classes for implementing the operator interface. They read
from incoming streams of tuples and write to other streams.

**Streams**

A stream is a connector (edge) abstraction which is a fundamental building
block of DataTorrent RTS. A stream consists of tuples that flow from one input
port to one or more output ports.

**Ports**

Ports are transient objects declared in the operator class and act connection
points for operators. Tuples flow in and out through ports. Input ports read
from streams while output port write to streams.

**Directed Acyclic Graph (DAG)**

A DAG is a logical representation of real-time stream processing application.
The computational units within a DAG are called operators and the data-flow
edges are called data streams.

**Logical Plan or DAG**

Logical Plan is the Data Object Model (DOM) that is created as operators and
streams are added to the DAG. It is identical to a DAG.

**Physical Plan or DAG**

A physical plan is the physical representation of the logical plan of the
application, which depicts how applications run on physical containers and
nodes of a DataTorrent cluster.

**Data Tuples Processed**

This is the number of data objects processed by real-time stream processing
applications.

**Data Tuples Emitted**

This is the number of data objects emitted after real-time stream processing
applications complete processing operations.

**Streaming Application Manager (STRAM)**

Streaming Application Manager (STRAM) is a YARN-native, lightweight controller
process. It is the process that is activated first upon application launch to
orchestrate the streaming application.

**Streaming Window**

A streaming window is a duration during which a set of tuples are emitted. The
collection of these tuples constitutes a window data set, also called as an
atomic micro-batch.

**Sliding Application Window**

Sliding window is computation that requires "n" streaming windows. After each
streaming window, the nth window is dropped, and the new window is added to the
computation.

**Demo Applications**

The real-time stream processing applications which are packaged with the
DataTorrent RTS binaries, are called demo applications. A Demo application can
be launched standalone, or on a Hadoop cluster.

**Command-line Interface**

Command line interface (CLI) is the access point for applications.
This is a wrapper around the web services layer, which makes the web
services user friendly.

**Web services**

DataTorrent RTS platform provides a robust webservices layer called
DT Gateway. Currently, Hadoop provides detailed web services for
map-reduce jobs. The DataTorrent RTS platform leverages the same
framework to provide a web service interface for real-time streaming
applications.
