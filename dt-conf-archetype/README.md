DataTorrent Configuration Maven Archetype
=========================================

How to Generate a DataTorrent Configuration Project Template
------------------------------------------------------------

Run the following command

    mvn archetype:generate -DarchetypeGroupId=com.datatorrent -DarchetypeArtifactId=dt-conf-archetype -DarchetypeVersion=2.1.0-SNAPSHOT -DgroupId=com.example -Dpackage=com.example.mydtapp -DartifactId=mydtconf -Dversion=1.0-SNAPSHOT

Using your favorite IDE, open the project that has just been created by the above command.
Write your application code and optionally operator code 

Change to the project directory created by the maven archetype and run mvn install

    cd mydtconf; mvn package

The DT App Package will be at target/DTConf-mydtconf.jar
