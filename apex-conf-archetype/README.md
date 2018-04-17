Apex App Configuration Maven Archetype
======================================

How to Generate a Apex App Configuration Project Template
---------------------------------------------------------

Run the following command

    mvn archetype:generate -DarchetypeGroupId=org.apache.apex -DarchetypeArtifactId=apex-conf-archetype -DarchetypeVersion=4.0.0-SNAPSHOT -DgroupId=com.example -Dpackage=com.example.myapexapp -DartifactId=myapexconf -Dversion=1.0-SNAPSHOT

Using your favorite IDE, open the project that has just been created by the above command.
Write your application code and optionally operator code 

Change to the project directory created by the maven archetype and run mvn package

    cd myapexconf; mvn package

The Apex App Configuration Package will be at target/myapexconf.apc
