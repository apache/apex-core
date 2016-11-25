This example application shows you how to use the launcher API to launch an Apex application on a YARN cluster. It
contains a simple Apex application along with the bootstrap code to launch it.

The application _main_ function in _Application.java contains the launch code. The _pom.xml_ build file 
has the necessary configuration to run the application. To launch the application, run the following maven command

```sh
mvn -Plaunch-app
```
