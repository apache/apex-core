Security
==========

Applications built on Apex run as native YARN applications on Hadoop. The security framework and apparatus in Hadoop apply to the applications. The default security mechanism in Hadoop is Kerberos.

Kerberos Authentication
-----------------------

Kerberos is a ticket based authentication system that provides authentication in a distributed environment where authentication is needed between multiple users, hosts and services. It is the de-facto authentication mechanism supported in Hadoop. To use Kerberos authentication, the Hadoop installation must first be configured for secure mode with Kerberos. Please refer to the administration guide of your Hadoop distribution on how to do that. Once Hadoop is configured, some configuration is needed on the Apex side as well.

Configuring security
---------------------

The Apex command line interface (CLI) program, `apex`, is used to launch applications on the Hadoop cluster along with performing various other operations and administrative tasks on the applications. In a secure cluster additional configuration is needed for the CLI program `apex`.

###CLI Configuration

  When Kerberos security is enabled in Hadoop, a Kerberos ticket granting ticket (TGT) or the Kerberos credentials of the user are needed by the CLI program `apex` to authenticate with Hadoop for any operation. Kerberos credentials are composed of a principal and either a _keytab_ or a password. For security and operational reasons only keytabs are supported in Hadoop and by extension in Apex platform. When user credentials are specified, all operations including launching application are performed as that user.

#### Using kinit

A Kerberos ticket granting ticket (TGT) can be obtained by using the Kerberos command `kinit`. Detailed documentation for the command can be found online or in man pages. An sample usage of this command is

    kinit -k -t path-tokeytab-file kerberos-principal

If this command is successful, the TGT is obtained, cached and available for other programs. The CLI program `apex` can then be started to launch applications and perform other operations.

#### Using Kerberos credentials

The CLI program `apex` can also use the Kerberos credentials directly without requiring a TGT to be obtained separately. This can be useful in batch mode where `apex` is not launched manually and also in scenarios where running another program like `kinit` is not feasible.

The credentials can be specified in the `dt-site.xml` configuration file. If only a single user is launching applications, the global `dt-site.xml` configuration file in the installation folder can be used. In a multi-user environment the users can use the `dt-site.xml` file in their
home directory. The location of this file will be `$HOME/.dt/dt-site.xml`. If this file does not exist, the user can create a new one.

The snippet below shows the how the credentials can be specified in the configuration file as properties.

```xml
<property>
        <name>dt.authentication.principal</name>
        <value>kerberos-principal-of-user</value>
</property>
<property>
        <name>dt.authentication.keytab</name>
        <value>absolute-path-to-keytab-file</value>
</property>
```

The property `dt.authentication.principal` specifies the Kerberos user principal and `dt.authentication.keytab` specifies the absolute path to the keytab file for the user.

### Web Services security

Alongside every Apex application is an application master process running called Streaming Container Manager (STRAM). STRAM manages the application by handling the various control aspects of the application such as orchestrating the execution of the application on the cluster, playing a key role in scalability and fault tolerance, providing application insight by collecting statistics among other functionality.

STRAM provides a web service interface to introspect the state of the application and its various components and to make dynamic changes to the applications. Some examples of supported functionality are getting resource usage and partition information of various operators, getting operator statistics and changing properties of running operators.

Access to the web services can be secured to prevent unauthorized access. By default it is automatically enabled in Hadoop secure mode environments and not enabled in non-secure environments. How the security actually works is described in `Security architecture` section below.

There are additional options available for finer grained control on enabling it. This can be configured on a per-application basis using an application attribute. It can also be enabled or disabled based on Hadoop security configuration. The following security options are available

* Enable - Enable Authentication
* Follow Hadoop Authentication - Enable authentication if secure mode is enabled in Hadoop, the default
* Follow Hadoop HTTP Authentication - Enable authentication only if HTTP authentication is enabled in Hadoop and not just secure mode.
* Disable - Disable Authentication

To specify the security option for an application the following configuration can be specified in the `dt-site.xml` file

```xml
<property>
        <name>dt.application.name.attr.STRAM_HTTP_AUTHENTICATION</name>
        <value>security-option</value>
</property>
```

The security option value can be `ENABLED`, `FOLLOW_HADOOP_AUTH`, `FOLLOW_HADOOP_HTTP_AUTH` or `DISABLE` for the four options above respectively.

The subsequent sections talk about how security works in Apex. This information is not needed by users but is intended for the inquisitive techical audience who want to know how security works.

### Token Refresh

Apex applications, at runtime, use delegation tokens to authenticate with Hadoop services when communicating with them as described in the security architecture section below. The delegation tokens are originally issued by these Hadoop services and have an expiry time period which is typically 7 days. The tokens become invalid beyond this time and the applications will no longer be able to communicate with the Hadoop services. For long running applications this presents a problem.

To solve this problem one of the two approaches can be used. The first approach is to change the Hadoop configuration itself to extend the token expiry time period. This may not be possible in all environments as it requires a change in the security policy as the tokens will now be valid for a longer period of time and the change also requires administrator privileges to Hadoop. The second approach is to use a feature available in apex to auto-refresh the tokens before they expire. Both the approaches are detailed below and the users can choose the one that works best for them.

####Hadoop configuration approach

An Apex application uses delegation tokens to authenticate with Hadoop services, Resource Manager (YARN) and Name Node (HDFS), and these tokens are issued by those services respectively. Since the application is long-running, the tokens can expire while the application is still running. Hadoop uses configuration settings for the maximum lifetime of these tokens. 

There are separate settings for ResourceManager and NameNode delegation tokens. In this approach the user increases the values of these settings to cover the lifetime of the application. Once these settings are changed, the YARN and HDFS services would have to be restarted. The values in these settings are of type `long` and has an upper limit so applications cannot run forever. This limitation is not present with the next approach described below.

The Resource Manager delegation token max lifetime is specified in `yarn-site.xml` and can be specified as follows for a lifetime of 1 year as an example

```xml
<property>
  <name>yarn.resourcemanager.delegation.token.max-lifetime</name>
  <value>31536000000</value>
</property>
```

The Name Node delegation token max lifetime is specified in
hdfs-site.xml and can be specified as follows for a lifetime of 1 year as an example

```xml
<property>
   <name>dfs.namenode.delegation.token.max-lifetime</name>
   <value>31536000000</value>
 </property>
```

####Auto-refresh approach

In this approach the application, in anticipation of a token expiring, obtains a new token to replace the current one. It keeps repeating the process whenever a token is close to expiry so that the application can continue to run indefinitely.

This requires the application having access to a keytab file at runtime because obtaining a new token requires a keytab. The keytab file should be present in HDFS so that the application can access it at runtime. The user can provide a HDFS location for the keytab file using a setting otherwise the keytab file specified for the `apex` CLI program above will be copied from the local filesystem into HDFS before the application is started and made available to the application. There are other optional settings available to configure the behavior of this feature. All the settings are described below.

The location of the keytab can be specified by using the following setting in `dt-site.xml`. If it is not specified then the file specified in `dt.authentication.keytab` is copied into HDFS and used.

```xml
<property>
        <name>dt.authentication.store.keytab</name>
        <value>hdfs-path-to-keytab-file</value>
</property>
```
The expiry period of the Resource Manager and Name Node tokens needs to be known so that the application can renew them before they expire. These are automatically obtained using the `yarn.resourcemanager.delegation.token.max-lifetime` and `dfs.namenode.delegation.token.max-lifetime` properties from the hadoop configuration files. Sometimes however these properties are not available or kept up-to-date on the nodes running the applications. If that is the case then the following properties can be used to specify the expiry period, the values are in milliseconds. The example below shows how to specify these with values of 7 days.

```xml
<property>
        <name>dt.resourcemanager.delegation.token.max-lifetime</name>
        <value>604800000</value>
</property>

<property>
        <name>dt.namenode.delegation.token.max-lifetime</name>
        <value>604800000</value>
</property>
```

As explained earlier new tokens are obtained before the old ones expire. How early the new tokens are obtained before expiry is controlled by a setting. This setting is specified as a factor of the token expiration with a value between 0.0 and 1.0. The default value is `0.7`. This factor is multipled with the expiration time to determine when to refresh the tokens. This setting can be changed by the user and the following example shows how this can be done

```xml
<property>
        <name>dt.authentication.token.refresh.factor</name>
        <value>0.7</value>
</property>
```

### Impersonation

The CLI program `apex` supports Hadoop proxy user impersonation, in allowing applications to be launched and other operations to be performed as a different user than the one specified by the Kerberos credentials. The Kerberos credentials are still used for authentication. This is useful in scenarios where a system using `apex` has to support multiple users but only has a single set of Kerberos credentials, those of a system user.

####Usage

To use this feature, the following environment variable should be set to the user name of the user being impersonated, before running `apex` and the operations will be performed as that user. For example, if launching an application, the application will run as the specified user and not as the user specified by the Kerberos credentials.

```
HADOOP_USER_NAME=<username>
```

####Hadoop Configuration

For this feature to work, additional configuration settings are needed in Hadoop. These settings would allow a specified user, such as a system user, to impersonate other users. The example snippet below shows these settings. In this example, the specified user can impersonate users belonging to any group and can do so running from any host. Note that the user specified here is different from the user specified above in usage, there it is the user that is being impersonated and here it is the impersonating user such as a system user.

```xml
<property>
  <name>hadoop.proxyuser.<username>.groups</name>
  <value>*</value>
</property>

<property>
  <name>hadoop.proxyuser.<username>.hosts</name>
  <value>*</value>
</property>
```

Security architecture
----------------------

In this section we will see how security works for applications built on Apex. We will look at the different methodologies involved in running the applications and in each case we will look into the different components that are involved. We will go into the architecture of these components and look at the different security mechanisms that are in play.

###Application Launch

To launch applications in Apache Apex the command line client `apex` can be used. The application artifacts such as binaries and properties are supplied as an application package. The client, during the various steps involved to launch the application needs to communicate with both the Resource Manager and the Name Node. The Resource Manager communication involves the client asking for new resources to run the application master and start the application launch process. The steps along with sample Java code are described in Writing YARN Applications. The Name Node communication includes the application artifacts being copied to HDFS so that they are available across the cluster for launching the different application containers.

In secure mode, the communications with both Resource Manager and Name Node requires authentication and the mechanism is Kerberos. Below is an illustration showing this.

![](images/security/image02.png)


The client `apex` supports Kerberos authentication and will automatically enable it in a secure environment. To authenticate, some Kerberos configuration namely the Kerberos credentials, are needed by the client. There are two parameters, the Kerberos principal and keytab to use for the client. These can be specified in the dt-site.xml configuration file. The properties are shown below

        <property>
                <name>dt.authentication.principal</name>
                <value>kerberos-principal-of-user</value>
        </property>
        <property>
                <name>dt.authentication.keytab</name>
                <value>absolute-path-to-keytab-file</value>
        </property>

Refer to document Operation and Installation Guide section Multi Tenancy and Security subsection CLI Configuration in the documentation for more information. The document can also be accessed here client configuration

There is another important functionality that is performed by the client and that is to retrieve what are called delegation tokens from the Resource Manager and Name Node to seed the application master container that is to be launched. This is detailed in the next section. 

###Runtime Security

When the application is completely up and running, there are different components of the application running as separate processes possibly on different nodes in the cluster as it is a distributed application. These components interactwould be interacting with each other and the Hadoop services. In secure mode, all these interactions have to be authenticated before they can be successfully processed. The interactions are illustrated below in a diagram to give a complete overview. Each of them is explained in subsequent sections.

![](images/security/image00.png)


####STRAM and Hadoop

Every Apache Apex application has a master process akin to any YARN application. In our case it is called STRAM (Streaming Application Master). It is a master process that runs in its own container and manages the different distributed components of the application. Among other tasks it requests Resource Manager for new resources as they are needed and gives back resources that are no longer needed. STRAM also needs to communicate with Name Node from time-to-time to access the persistent HDFS file system. 

In secure mode, STRAM has to authenticate with both Resource Manager and Name Node before it can send any requests and this is achieved using Delegation Tokens. Since STRAM runs as a managed application master, it runs in a Hadoop container. This container could have been allocated on any node based on what resources were available. Since there is no fixed node where STRAM runs, it does not have Kerberos credentials. Unlike launch client `apex`, it cannot authenticate with Hadoop services Resource Manager and Name Node using Kerberos. Instead, Delegation Tokens are used for authentication.

#####Delegation Tokens

Delegation tokens are tokens that are dynamically issued by the source and clients use them to authenticate with the source. The source stores the delegation tokens it has issued in a cache and checks the delegation token sent by a client against the cache. If a match is found, the authentication is successful else it fails. This is the second mode of authentication in secure Hadoop after Kerberos. More details can be found in the Hadoop security design document. In this case the delegation tokens are issued by Resource Manager and Name Node. STRAM would use these tokens to authenticate with them. But how does it get them in the first place? This is where the launch client `apex` comes in.

The client `apex`, since it possesses Kerberos credentials as explained in the Application Launch section, is able to authenticate with Resource Manager and Name Node using Kerberos. It then requests for delegation tokens over the Kerberos authenticated connection. The servers return the delegation tokens in the response payload. The client in requesting the resource manager for the start of the application master container for STRAM seeds it with these tokens so that when STRAM starts it has these tokens. It can then use these tokens to authenticate with the Hadoop services.

####Streaming Container

A streaming container is a process that runs a part of the application business logic. It is a container deployed on a node in the cluster. The part of business logic is implemented in what we call an operator. Multiple operators connected together make up the complete application and hence there are multiple streaming containers in an application. The streaming containers have different types of communications going on as illustrated in the diagram above. They are described below.

#####STRAM Delegation Token

The streaming containers periodically communicate with the application master STRAM. In the communication they send what are called heartbeats with information such as statistics and receive commands from STRAM such as deployment or un-deployment of operators, changing properties of operators etc. In secure mode, this communication cannot just occur without any authentication. To facilitate this authentication special tokens called STRAM Delegation Tokens are used. These tokens are created and managed by STRAM. When a new streaming container is being started, since STRAM is the one negotiating resources from Resource Manager for the container and requesting to start the container, it seeds the container with the STRAM delegation token necessary to communicate with it. Thus, a streaming container has the STRAM delegation token to successfully authenticate and communicate with STRAM.

#####Buffer Server Token

As mentioned earlier an operator implements a piece of the business logic of the application and multiple operators together complete the application. In creating the application the operators are assembled together in a direct acyclic graph, a pipeline, with output of operators becoming the input for other operators. At runtime the stream containers hosting the operators are connected to each other and sending data to each other. In secure mode these connections should be authenticated too, more importantly than others, as they are involved in transferring application data.

When operators are running there will be effective processing rate differences between them due to intrinsic reasons such as operator logic or external reasons such as different resource availability of CPU, memory, network bandwidth etc. as the operators are running in different containers. To maximize performance and utilization the data flow is handled asynchronous to the regular operator function and a buffer is used to intermediately store the data that is being produced by the operator. This buffered data is served by a buffer server over the network connection to the downstream streaming container containing the operator that is supposed to receive the data from this operator. This connection is secured by a token called the buffer server token. These tokens are also generated and seeded by STRAM when the streaming containers are deployed and started and it uses different tokens for different buffer servers to have better security.

#####NameNode Delegation Token

Like STRAM, streaming containers also need to communicate with NameNode to use HDFS persistence for reasons such as saving the state of the operators. In secure mode they also use NameNode delegation tokens for authentication. These tokens are also seeded by STRAM for the streaming containers.

#### Stram Web Services

Clients connect to STRAM and make web service requests to obtain operational information about running applications. When security is enabled we want this connection to also be authenticated. In this mode the client passes a web service token in the request and STRAM checks this token. If the token is valid, then the request is processed else it is denied.

How does the client get the web service token in the first place? The client will have to first connect to STRAM via the Resource Manager Web Services Proxy which is a service run by Hadoop to proxy requests to application web services. This connection is authenticated by the proxy service using a protocol called SPNEGO when secure mode is enabled. SPNEGO is Kerberos over HTTP and the client also needs to support it. If the authentication is successful the proxy forwards the request to STRAM. STRAM in processing the request generates and sends back a web service token similar to a delegation token. This token is then used by the client in subsequent requests it makes directly to STRAM and STRAM is able to validate it since it generated the token in the first place.

![](images/security/image03.png)

Conclusion
-----------

We looked at the different security configuration options that are available in Apex, saw the different security requirements for distributed applications in a secure Hadoop environment in detail and looked at how the various security mechanisms in Apex solves this.
