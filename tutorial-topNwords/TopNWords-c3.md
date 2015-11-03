Building top N words using dtAssemble
===
You can build top N words using dtAssemble &ndash; the graphical drag-and-drop
application builder.

_Note_: This tool is not available with the Community edition license.

Using the application builder, you can manually drag and drop participating operators on to the application canvas and connect them to build the DAG for
the top N words application. You can move the application canvas in any
direction using your mouse. You can also try the auto-layout and zoom-to-fit
buttons at the top-right of the canvas to create an initial arrangement.

_Note_: You cannot undo the auto-layout or zoom-to-fit operations.

Prerequisites

To use **dtAssemble**, you should have uploaded an application package that
contains all the operators you intend to use. The new application will reside
in that package. For this exercise, we will use the package that you built and
uploaded in the earlier chapter.

To ensure that the full complement of sandbox resources are available for
running the current application, ensure that no other applications are running
by clicking the _Monitor_ tab, and under _DataTorrent Applications_, kill any
running applications.

Step I: Create top N words using dtAssemble
---

1. Log on to the DataTorrent RTS console (default username and password are
   both `dtadmin`).
2. On the DataTorrent RTS console, click _Develop_ &#x21e8; _App Packages_.
3. Make sure that the top N words package that you built following the
   instructions of the previous chapter is uploaded.
4. Click _TopNWordCount_ in the name column to see the application details.
  ![TopNWordCount](images/image38.png "TopNWordCount")
5. Click _create new application_ button.
  ![New Application](images/image37.png "New Application")
6. Type a name for your application, for example, _Top N words_, and click
   _Create_. The **Application Canvas** should open.

The existing application is a JAVA application. Applications that you create
using dtAssemble are JSON applications. For each JSON application, there are
three additional buttons that allow editing, deleting, and cloning operations.
These operations are not supported for JAVA applications.

![JSON Application](images/image40.png "JSON Application")

Step II: Drag operators to the application canvas
---

1. Wait till the Application Canvas opens.
  ![Application Canvas](images/image39.png "Application Canvas")
2. From the Operator Library list on the left, locate the desired
   operators by either:
3. Exploring the categories.

    -or-

4. Using the _quick find_ feature by typing the first few letters of the name of
   the implementing class or related terms in the search box. For example, to
   find the file reader, type _file_  into the search box to see a list of
   matching operators. For example, the first operator is _LineReader_.
    ![Operator Library](images/image43.png "Operator Library")
4. Drag the operator onto the canvas.
5. Repeat this process for all the operators described in Appendix entitled
    _Operators in Top N words application_, and arrange them on the canvas.
6. To magnify or shrink the operators, use the buttons in the top-right corner.
   You can also use the scroll wheel of your mouse to zoom in or out. The
   entire canvas can also be moved in any direction with the mouse.

Step III: Connect the operators
---

Observe that each operator shows output ports in pink and input ports in blue.
The names of the operators and the corresponding JAVA classes are also shown.
You can change the operator name by clicking it, and then changing the name in
the Operator Name box in the Operator Inspector panel.

Connect the operators as shown in the diagram below. Note the following points about
the _FileWordCount_ operator which has the largest number of connections:

- The control port is connected to the control port of _LineReader_.
- The input port is connected to the output port of _WindowWordCount_.
- The _fileOutput_ port emits the final top N pairs to the corresponding
  output file and so is connected to the input port of _WordCountWriter_.
- The _outputGlobal_ port emits the global top N pairs and so is connected to
  the input port of _AppDataSnapshotMapServer_.
- The _outputPerFile_ port emits the top N pairs for the current file (while
  the file is still being read) and so it is connected to _ConsoleOutput_ as
  well as to _AppDataSnapshotMapServer_.

_Note_: As you make changes, the top left corner displays _All
changes saved to HDFS_. No explicit save step is needed.

After you connect all the operators, the canvas looks like this.
Although presented differently, it is the same as the logical DAG for
the Java application.

![Connections](images/image41.png "Connections")

Step IV: Configure the operator properties
---
The last step before running this application is to configure
properties of the operators.

1. Click the first operator (_LineReader_) in the canvas to see the list of
   configurable properties in the right panel.
2. Locate the property labelled _Directory_ and enter the path to the input
   directory: `/tmp/test/input-dir`:

    ![Properties](images/image42.png "Properties")

3. Configure the properties of the remaining operators using this table for
   reference. The table contains the same values that we set in the properties
   file for the Java application.

    <table>
    <colgroup>
    <col width="10%" />
    <col width="30%" />
    <col width="60%" />
    </colgroup>
    <tbody>
    <tr class="odd">
    <td align="left"><p>**Operator**</p></td>
    <td align="left"><p>**Property Name**</p></td>
    <td align="left"><p>**Value**</p></td>
    </tr>
    <tr class="even">
    <td align="left"><p>5</p></td>
    <td align="left"><p>File Path</p></td>
    <td align="left"><p>/tmp/test/output-dir</p></td>
    </tr>
    <tr class="odd">
    <td align="left"><p>2</p></td>
    <td align="left"><p>Non Word Str</p></td>
    <td align="left"><p>[\p{Punct}\s]+</p></td>
    </tr>
    <tr class="even">
    <td align="left"><p>9</p></td>
    <td align="left"><p>Topic</p></td>
    <td align="left"><p>TopNWordsQueryFile</p></td>
    </tr>
    <tr class="odd">
    <td align="left"><p>10</p></td>
    <td align="left"><p>Topic</p></td>
    <td align="left"><p>TopNWordsQueryGlobal</p></td>
    </tr>
    <tr class="even">
    <td align="left"><p>11</p></td>
    <td align="left"><p>Topic</p></td>
    <td align="left"><p>TopNWordsQueryFileResult</p></td>
    </tr>
    <tr class="odd">
    <td align="left"><p>12</p></td>
    <td align="left"><p>Topic</p></td>
    <td align="left"><p>TopNWordsQueryGlobalResult</p></td>
    </tr>
    <tr class="even">
    <td align="left"><p>7, 8</p></td>
    <td align="left"><p>Snapshot Schema JSON</p></td>
    <td align="left"><p>{ &quot;values&quot;: [{&quot;name&quot;: &quot;word&quot;, &quot;type&quot;: &quot;string&quot;},</p>
    <p>{&quot;name&quot;: &quot;count&quot;, &quot;type&quot;: &quot;integer&quot;}] }</p></td>
    </tr>
    <tr class="odd">
    <td align="left"><p>4</p></td>
    <td align="left"><p>Top N</p></td>
    <td align="left"><p>10</p></td>
    </tr>
    </tbody>
    </table>

4. Click Stream 8 and Stream 9, and change _Stream Locality_ from
  `AUTOMATIC` to `CONTAINER_LOCAL` to match our earlier properties file.
  After you perform this step, the line for the stream will change
  from a solid line to a dotted line.

5. Click the blank area of the canvas to see the
   application attributes in the right panel. Scroll to _Master Memory Mb_, and
   change its value to 500.

6. Click each operator, navigate to the _Memory Mb_ attribute in the
   _Attributes_ section, and change the value to 200 except for _Operator
    4_ for which the value is 512.

7. Click _launch_ in the top-left corner. _Note_: Before launching the
   application, shut down the IDE; if it is running at the time of a launch,
   the sandbox might hang due to resource exhaustion.
  ![launch](images/image44.png "launch")

8. On the launch application dialog window, type a name for your application.
  ![Name](images/image45.png "Name")

9. (Optional) To configure the application using a configuration file, select
    _Use a config file_ checkbox. To specify individual properties, select
    _Specify custom properties_ checkbox.
10.  Click _Launch_.

A transient pop-up at the top-right indicating that the launch was successful
should appear.
![Launched](images/image46.png "Launched")

After a successful launch, monitor the application following
instructions in the Chapter entitled _Monitoring with dtManage_.
