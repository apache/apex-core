DataTorrent Application Maven Archetype
=======================================

How to Generate a DataTorrent Application Project Template
----------------------------------------------------------

Run the following command

    mvn archetype:generate -DarchetypeGroupId=com.datatorrent -DarchetypeArtifactId=dt-app-archetype -DarchetypeVersion=1.0.4-SNAPSHOT -DgroupId=com.example -Dpackage=com.example.mydtapp -DartifactId=mydtapp -Dversion=1.0-SNAPSHOT

Using your favorite IDE, open the project that has just been created by the above command.
Write your application code and optionally operator code 

Change to the project directory created by the maven archetype and run mvn install

    cd mydtapp; mvn install

The DT App Package will be at target/dtapp-mydtapp-1.0-SNAPSHOT.jar
