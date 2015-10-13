DataTorrent RTS Sandbox Demo Applications
================================================================================

DataTorrent RTS includes a number of pre-loaded demo applications, which help demonstrate the features of the RTS platform.


Basic Demo Applications
--------------------------------------------------------------------------------

These applications require minimal resources and configuration changes and can be launched with a single click.  Ensure Hadoop [YARN](http://localhost:8088/cluster/cluster) and [HDFS](http://localhost:50070/dfshealth.jsp) are active and ready before launching DataTorrent demo applications.

1.  Navigate to <a href="http://localhost:9090/#/dev/packages" target="_blank">App Packages</a> under Develop tab of the DataTorrent console, and select one of the demo packages.  In this example we will use **PiDemo** application package.

2.  From the list of available Applications, locate PiDemo and press the launch button.
    
    ![](images/sandbox/pidemo-list.png)

3.  Proceed with default options on launch confirmation screen by pressing the Launch button.

4.  Once launched, view the running application by following the link provided in the launch confirmation dialog, or by navigating to the <a href="http://localhost:9090/#/ops" target="_blank">Monitor</a> section of the Console and selecting the launched application.

    ![](images/sandbox/pidemo-success.png)

More information about using DataTorrent Console is available in [Console Guide](https://www.datatorrent.com/docs/guides/ConsoleGuide.html)



Advanced Demo Applications
--------------------------------------------------------------------------------

These applications may require additional configuration changes prior to launching.  Configuration changes can be made on the launch confirmation screen or manually applied to `~/.dt/dt-site.xml` configuration file.  These typically include adding Twitter API keys for twitter demo, or changing performance settings for larger applications.  Guides for various demo applications can be found in the [docs](http://docs.datatorrent.com/).

1.  Navigate to [App Packages](http://localhost:9090/#/packages) tab of the DataTorrent console, and select one of the demo packages.  In this example we will use [Twitter Demo](http://localhost:9090/#/packages) application package.

2.  From the list of Applications, select TwitterDemo and press the corresponding launch button.

3.  Retrieve Twitter API access information by registering for [Twitter Developer](https://dev.twitter.com/) account, creating a new [Twitter Application](https://apps.twitter.com/app/new), and navigating to Keys and Access Tokens tab.  Twitter Demo application requires the following to be specified by the user:

    dt.operator.TweetSampler.accessToken
    dt.operator.TweetSampler.accessTokenSecret
    dt.operator.TweetSampler.consumerKey
    dt.operator.TweetSampler.consumerSecret

4.  Select *Specify custom properties* on the launch confirmation screen, click *add required properties* button, and provide Twitter API access values.  Choose to save this configuration as `twitter.xml` file and proceed to Launch the application.

    ![](images/sandbox/twitterdemo-launch.png)

5.  Once launched, view the running application by following the link provided in the launch confirmation dialog, or by navigating to DataTorrent [Console](http://localhost:9090/#/ops) and selecting the launched application.

6.  View the top 10 tweeted hashtags in real time by generating and viewing the [dashboards](http://localhost:9090/#/dashboards).



Stopping Applications
--------------------------------------------------------------------------------

Applications can be shut down or killed from the [DataTorrent Console](/#ops) by selecting application from the list and clicking `shutdown` or `kill` buttons.
