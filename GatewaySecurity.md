# DataTorrent Gateway Security


Introduction
============

DataTorrent Gateway supports different authentication mechanisms to
secure access to the Console. The supported types are local password
authentication, kerberos authentication and J

Configuring Authentication
==========================

 

After DataTorrent RTS installation, you can turn on authentication and
authorization support to secure the DataTorrent Gateway. Gateway
supports three types of authentication.

-   Password
-   Kerberos
-   JAAS

Enabling Password Auth
----------------------

“Password” is the only authentication mechanism presented here that does
not depend on any external systems.  When enabled, all users will be
presented with the login prompt before being able to use the DT Console.
Password authentication can be enabled by performing following two
steps.

1.  Add a property to `dt-site.xml` configuration file, typically located
    under `/opt/datatorrent/current/conf` ( or
    `~/datatorrent/current/conf` for local install).

```
<configuration>
...
<property>
          <name>dt.gateway.http.authentication.type</name>
          <value>password</value>
</property>
...
</configuration>
```

1.  Restart the Gateway by running

`sudo service dtgateway restart`

( when running Gateway in local mode use  dtgateway restart command)

Open the Gateway URL in your browser, and you should be prompted for
user name and password.  Starting with DataTorrent RTS 2.0.0, the
default username and password is **dtadmin** and **dtadmin**.

![](images/GatewaySecurity/image02.png)

Once authenticated, active user name and an option to log out is
presented in the top right corner of the DT Console screen.

![](images/GatewaySecurity/image00.png)

Enabling Kerberos Auth
----------------------

Kerberos authentication can optionally be enabled for Hadoop web access.
If this is configured then all web browser access to the Hadoop
management consoles is Kerberos authenticated. Access to all the web
services is also Kerberos authenticated. This Kerberos authentication is
performed using a protocol called SPNEGO which is Kerberos over HTTP.
Please refer to the administration guide of your Hadoop distribution on
how to enable this. The web browsers must also support SPNEGO, most
modern browsers do and should be configured to use SPNEGO for the
specific URLs being accessed. Please refer to the documentation of the
individual browsers on how to configure this. Gateway handles the SPNEGO
authentication needed when communicating with the Hadoop web-services if
Kerberos authentication is enabled for Hadoop web access.

Kerberos authentication can be enabled for UI Console as well. When it
is enabled access to the Gateway web-services is Kerberos authenticated.
The browser should be configured for SPNEGO authentication when
accessing the Console. A user typically obtains a master ticket first by
logging in to kerberos system in a terminal emulator using kinit. Then
the user launches a browser and accesses the Console URL. The browser
will use the master ticket obtained by kinit earlier to obtain the
necessary security tokens to authenticate with the Gateway.

When this authentication is enabled the first authenticated user that
accesses the system is assigned the admin role as there are no other
users in the system at this point. Any subsequent authenticated user
that access the system starts with no roles. The admin user can then
assign roles to these users. This behavior can be changed by configuring
an external role mapping. Please refer to the [External Role
Mapping](#ExternalRoleMapping) in the [Authorization using external
roles](#ExternalRoles) section below for that.

Additional configuration is needed to enable Kerberos authentication for
the Console. A separate set of kerberos credentials are needed. These
can be same as the Hadoop web-service Kerberos credentials which are
typically identified with the principal HTTP/HOST@DOMAIN. A few other
configuration properties are also needed. These can be specified in the
same “dt-site.xml” configuration file as the DT Gateway authentication
configuration described in the Operation and Installation Guide. This
authentication can be set up using the following steps.

1.  Add the following properties to `dt-site.xml` configuration file, typically located under `/opt/datatorrent/current/conf` ( or `~/datatorrent/current/conf` for local install)

```
<configuration>
...
   <property>
             <name>dt.gateway.http.authentication.type</name>
             <value>kerberos</value>
   </property>
   
   <property>
          <name>dt.gateway.http.authentication.kerberos.principal</name>
          <value>{kerberos-principal-of-web-service}</value>
   </property>
   
   <property>
	 <name>dt.gateway.http.authentication.kerberos.keytab</name>
         <value>{absolute-path-to-keytab-file}</value>
   </property>

   <property>
         <name>dt.gateway.http.authentication.token.validity</name>
         <value>{authentication-token-validity-in-seconds}</value>
   </property>
   
    <property>
         <name\>dt.gateway.http.authentication.cookie.domain</name>
         <value\>{http-cookie-domain-for-authentication-token}</value>

    <property>
          <name\>dt.gateway.http.authentication.cookie.path</name>
                   <value>{http-cookie-path}</value>
    </property>
    
    <property>
           <name\>dt.gateway.http.authentication.signature.secret</name>
		          <value>{absolute-path-of-secret-file-for-signing-authentication-tokens} </value>
    </property>
...
</configuration>
```
(Note that the kerberos principal for web service should begin with
HTTP/…)

All the values for the properties above except for the property
`dt.gateway.http.authentication.type` should be replaced with the
appropriate values for your setup.

1.  Restart the Gateway by running

sudo service dtgateway restart

( when running Gateway in local mode use  `dtgateway restart` command)

Enabling JAAS Auth
------------------

JAAS is Java Authentication and Authorization Service. It is a pluggable
and extensible mechanism for authentication. It is a generic framework
and the actual authentication is performed by a JAAS plugin module which
can be configured using a configuration file. Among others LDAP and PAM
authentication can be performed via JAAS.

Similar to Kerberos when this authentication is enabled the first
authenticated user that accesses the system is assigned the admin role
as there are no other users in the system at this point. Any subsequent
authenticated user that access the system starts with no roles. The
admin user can then assign roles to these users. This behavior can be
changed by configuring an external role mapping. Please refer to the
[External Role Mapping](#h.ijx5fx5wv0nd) in the [Authorization using
external roles](#h.v7j9m747yamt) section below for that.

This authentication can be enabled by performing the following general
steps. The specific steps for a couple of authentication mechanisms LDAP
and PAM are shown in the next sections but other authentication
mechanisms that have a JAAS plugin module can also be used.

1.  Add a property to `dt-site.xml` configuration file, typically located
    under `/opt/datatorrent/current/conf` ( or
    `~/datatorrent/current/conf` for local install).

```
<configuration>
...
     <property>
          <name>dt.gateway.http.authentication.type</name>
          <value>jaas</value>
     </property>
     <property\>
          <name>dt.gateway.http.authentication.jaas.name</name>
          <value>name-of-jaas-module</value>
     </property>
...
</configuration>
```

The `dt.gateway.http.authentication.jaas.name` property specifies the
plugin module to use with JAAS and the value should be the name of the
plugin module.

1.  The name of the plugin module specified above should be configured
    with the appropriate settings for the plugin. This is module
    specific configuration. This can be done in a file named
    .java.login.config in the home directory of the user Gateway is
    running under. If DataTorrent RTS was installed as a superuser,
    Gateway would typically run as dtadmin and this file path would
    typically be `/home/dtadmin/.java.login.config`, if running as a
    normal user it would be `~/.java.login.config`. The sample
    configurations for LDAP and PAM are shown in the next sections.

1.  The classes for the plugin module may need to be made available to
    Gateway if they are not available in the default classpath. This can
    be done by specifying the jars containing these classes in a
    classpath variable that is used by Gateway.

The following step shows how to do this

1.  Edit the `custom-env.sh` configuration file, typically located under
    `/opt/datatorrent/current/conf` ( or `~/datatorrent/current/conf` for
    local install) and append the list of jars obtained above to the
    DT\_CLASSPATH variable. This needs to be added at the end of the
    file in the section for specifying local overrides to environment
    variables. The line would look like

  `export DT\_CLASSPATH=\${DT\_CLASSPATH}:path/to/jar1:path/to/jar2:..`

1.  Restart the Gateway by running

`sudo service dtgateway restart`

( when running Gateway in local mode use  `dtgateway restart` command)

### LDAP

LDAP authentication is a directory based authentication mechanism used
in many enterprises. To enable LDAP authentication following are the
specifics for the configuration steps described above.

-   For step 1 above specify LDAP as the authentication module to use
    with JAAS by first specifying the value of the
    dt.gateway.http.authentication.jaas.name property above to be “ldap”
    (without the quotes). This name should now be configured with the
    appropriate settings as described in the next step.

-   For step 2, the JAAS name specified above should be configured with
    the appropriate LDAP settings in the .java.login.config file. A
    sample configuration is shown below

```ldap {
   com.sun.security.auth.module.LdapLoginModule required
userProvider="ldap://ldap-server-hostname"
authIdentity="uid={USERNAME},ou=users,dc=domain,dc=com";
}; ```

  Note that the first string before the open brace, in this case
“ldap” must match the jaas name specified in step 1. The first property
within the braces ```com.sun.security.auth.module.LdapLoginModule``` specifies
the actual JAAS plugin implementation class providing the LDAP
authentication. The next settings are LDAP settings specific to your
organization and could be different from ones shown above. The above
settings are only provided as a reference example.

### PAM

PAM is Pluggable Authentication Module. It is a Linux system equivalent
of JAAS where applications call the generic PAM interface and the actual
authentication implementations called PAM modules are specified using
configuration files. PAM is the login authentication mechanism in Linux
so it can be used for example to authenticate and use local Linux user
accounts in Gateway. If organizations have configured other PAM modules
they can be used in Gateway as well.

PAM is implemented in C language and has C API. JPam is Java PAM bridge
that uses JNI to make PAM calls. It is available here
[http://jpam.sourceforge.net/](http://jpam.sourceforge.net/) and has detailed documentation on how to install and set it up. JPam also has a JAAS plugin module and hence can be used in Gateway via JAAS. Note that any other PAM implementation can be used as long as a JAAS plugin module is available.

To enable JPAM following are the specifics for the configuration steps
to enable JAAS authentication described above.

-   JPAM has to be first installed on the system. Please following the
    installation instructions from the JPAM website.

-   For step 1 above Specify JPAM as the authentication module to use
    with JAAS by first specifying the value of the
    dt.gateway.http.authentication.jaas.name property above to be
    “net-sf-jpam” (without the quotes). This name should now be
    configured with the appropriate settings as described in the next
    step.

-   For step 2 the JAAS name specified above should be configured with
    the appropriate JPAM settings in the .java.login.config file. A
    sample configuration is shown below

net-sf-jpam {

   net.sf.jpam.jaas.JpamLoginModule required serviceName="net-sf-jpam";

}; 


Note that the first string before the open brace, in this case
“net-sf-jpam” must match the jaas name specified in step 1. The first
property within the braces net.sf.jpam.jaas.JpamLoginModule specifies
the actual JAAS plugin implementation class providing the JPAM
authentication. The next settings are JPAM specific settings. The
serviceName setting for example specifies the PAM service which would
need to be further configured in /etc/pam.d/net-sf-jpam to specify the
PAM modules to use. Refer to PAM documentation on how to configure a PAM
service with PAM modules. If using Linux local accounts system-auth
could be specified as the PAM module in this file. The above settings
are only provided as a reference example and a different serviceName for
example can be chosen.

-   For step 3 add the JPam jar to the DT\_CLASSPATH variable. The JPam
    jar should be available in the JPam installation package and
    typically has the filename format ```JPam-<version>.jar``` where
   ```<version>``` denotes the version, version 1.1 has been tested.
```export DT\_CLASSPATH=\${DT\_CLASSPATH}:path/to/JPam-\<version\>.jar```

### Groups

For group support such as using LDAP groups for authorization refer to
the [Authorization using external roles](#h.v7j9m747yamt) section below.



Configuring Authorization
=========================

When any authentication method is enabled, authorization will also be
enabled.  DT Gateway uses roles and permissions for authorization.
 Permissions are possession of the authority to perform certain actions,
e.g. to launch apps, or to add users.  Roles have a collection of
permissions and individual users are assigned to one or more roles.
 What roles the user is in dictates what permissions the user has.

Permissions
-----------

The list of all possible permissions in the DT Gateway is as follow:

### ACCESS\_RM\_PROXY

Allow HTTP proxying requests to YARN’s Resource Manager REST API

### EDIT\_GLOBAL\_CONFIG

Edit global settings

### EDIT\_OTHER\_USERS\_CONFIG

Edit other users’ settings

### LAUNCH\_APPS

Launch Apps

### MANAGE\_LICENSES

Manage DataTorrent RTS licenses

### MANAGE\_OTHER\_USERS\_APPS

Manage (e.g. edit, kill, etc) applications launched by other users

### MANAGE\_OTHER\_USERS\_APP\_PACKAGES

Manage App Packages uploaded by other users

### MANAGE\_ROLES

Manage roles (create/delete roles, or assign permissions to roles)

### MANAGE\_SYSTEM\_ALERTS

Manage system alerts

### MANAGE\_USERS

Manage users (create/delete users, change password)

### UPLOAD\_APP\_PACKAGES

Upload App Packages and use the app builder

### VIEW\_GLOBAL\_CONFIG

View global settings  

### VIEW\_LICENSES

View DataTorrent RTS licenses

### VIEW\_OTHER\_USERS\_APPS

View applications launched by others

### VIEW\_OTHER\_USERS\_APP\_PACKAGES

View App Packages uploaded by other users

### VIEW\_OTHER\_USERS\_CONFIG

Edit other users’ settings

### VIEW\_SYSTEM\_ALERTS

View system alerts

Default Roles
-------------

DataTorrent RTS ships with three roles by default. The permissions for
these roles have been set accordingly but can be customized if needed.

### Admin

An administrator of DataTorrent RTS is intended to be able to install,
manage & modify DataTorrent RTS as well as all the applications running
on it. They have ALL the permissions.

### Operator

Operators are intended to ensure DataTorrent RTS and the applications
running on top of it are always up and running optimally. The default
permissions assigned to Operators, enable them to effectively
troubleshoot the entire RTS system and the applications running on it.
Operators are not allowed however to develop or launch new applications.
Operators are also not allowed to make system configuration changes or
manage users.


Here is the list of default permissions given to operators

[MANAGE\_SYSTEM\_ALERTS](#h.pn7iwc10y8xy)

[VIEW\_GLOBAL\_CONFIG](#h.tluh45mlsx72)

[VIEW\_LICENSES](#h.76u6h88s274s)

[VIEW\_OTHER\_USERS\_APPS](#h.rfl0ve2p50ah)

[VIEW\_OTHER\_USERS\_APP\_PACKAGES](#h.lthaz1a2bs9g)

[VIEW\_SYSTEM\_ALERTS](#h.dmgh5z3fotop)

Note that VIEW\_OTHER\_USERS\_APPS and VIEW\_OTHER\_USERS\_APP\_PACKAGES
are in the list.  This means all users in the “operator” role will have
read access to all apps and all app packages in the system.  You can
remove those permissions from the “operator” role using the Console if
this is not desirable.

### Developer

Developers need to have to ability to develop, manage and run
applications on DataTorrent RTS. They are not allowed to change any
system settings or manage licenses.

Here is the list of default permissions given to developers

[](#h.89bx5fr3fvg)

[LAUNCH\_APPS](#h.eyevjyuu3wvm)

[UPLOAD\_APP\_PACKAGES](#h.pd46zl50kb2k)

[MANAGE\_SYSTEM\_ALERTS](#h.pn7iwc10y8xy)

[VIEW\_GLOBAL\_CONFIG](#h.tluh45mlsx72)

[VIEW\_LICENSES](#h.76u6h88s274s)

[VIEW\_SYSTEM\_ALERTS](#h.dmgh5z3fotop)

App Permissions and App Package Permissions
-------------------------------------------

Users can share their running application instances and their
application packages with certain roles or certain users on a
per-instance or per-package basis.  Users can specify which users or
while roles have read-only or read-write access.  In addition, users can
set their own defaults so they does not have to make permission change
every time they launch an app or uploads an app package.

The default for app and app package sharing is that the “operator” role
has read-only permission.

As of RTS 2.0.0, the Console does not yet support managing App
Permissions or App Package Permissions.  But one can manage App
Permissions and App Package Permissions using the Gateway REST API with
URI’s /ws/v2/apps/{appid}/permissions and
/ws/v2/appPackages/{user}/{name}/permissions respectively.  Please refer
to the [DT Gateway REST API document](https://www.datatorrent.com/docs/guides/DTGatewayAPISpecification.html) and [here](#h.zfqbod7fk3oc) for examples on how to use the REST API.


Viewing and Managing Auth in the Console
========================================

Viewing User Profile
--------------------

After you are logged in on the Console, click on the Configuration tab
on the left, and select “User Profile”.  This gives you the information
of the logged in user, including what roles the user is in, and what
permissions the user has.

![](images/GatewaySecurity/image01.png)

Administering Auth
------------------

From the Configuration tab, click on “Auth Management”.  On this page,
you can perform the following tasks:

-   Create new users
-   Delete users
-   Change existing users’ passwords
-   Assign roles to users
-   Create roles
-   Assign permissions to roles

![](images/GatewaySecurity/image03.png)

DataTorrent RTS installation comes with three preset roles (admin,
operator, and developer).  You can edit the permissions for those roles
except for admin.

* * * * *

<a name="ExternalRoles"></a> Authorization using external roles
==================================

When using an external authentication mechanism such as Kerberos or
JAAS, roles defined in these external systems can be used to control
authorization in DataTorrent RTS. There are two steps involved. First
support for external roles has to be configured in Gateway. This is
described below in the sections [Kerberos roles](#Kerberos) and
[JAAS roles](#JAAS). Then a mapping should be specified between
the external roles and DataTorrent roles to specify which role should be
used for a user when the user logs in. How to setup this mapping is
described in the [External Role Mapping](#ExternalRoleMapping) section below.
When this mapping is setup only users with roles that have a mapping are
allowed to login the rest are not. The next sections describe how to
configure the system for handling external roles.

<a name="Kerberos">Kerberos roles
--------------

When Kerberos authentication is used the role for the user is derived
from the principal. If the principal is of the form *user/group@DOMAIN*
the group portion is used as the external role and no additional
configuration is necessary.

<a name="JAAS">JAAS roles
----------

To use JAAS roles the system should be configured first to recognize
these roles. When a user is authenticated with JAAS a list of principals
is returned for the user by the implementing JAAS authentication module.
Some of these principals can be for roles and these role principals need
to be identified from the list. Additional configuration is needed to do
this and this configuration is specific to the JAAS authentication
module being used. Specifically the JAVA class name identifying the role
principal needs to be specified. This can be specified using the
property *“dt.gateway.http.authentication.jaas.role.class.name”* in the
configuration file as shown below

```
<configuration>
...
  <property> 
       <name>dt.gateway.http.authentication.jaas.role.class.name</name>
          <value>full-class-name-of-role</value>
 </property>
...
</configuration>
```

### Callback Handlers

In JAAS authentication, a login module may need a custom callback to be
handled by the caller. The callbacks are typically used to provide
authentication credentials to the login module. DataTorrent RTS supports
specification of a custom callback handler to handle these callbacks.
The custom callback handler can be specified using the property
“dt.gateway.http.authentication.jaas.callback.class.name”. When this
property is not specified a default callback handler is used but it may
not be sufficient for all login modules like in the LDAP case described
below. The property can be specified as follows

```
<configuration>
...
<property>                                                                            <name>dt.gateway.http.authentication.jaas.callback.class.name</name>
<value>full-class-name-of-callback</value>
</property>
...
</configuration>
```
Custom callback handlers can be implemented by extending the default
callback handler so they can build upon it or they can be built from
scratch. The LDAP callback handler described below also extends the
default callback handler. The source for the default callback handler
can be found here *DefaultCallbackHandler* can be used as a reference when
implementing new callback handlers.

### LDAP

When using LDAP with JAAS, to use LDAP roles, a  LDAP login module
supporting roles should be used. Any LDAP module that supports roles can
be used. Jetty implements one such login module. The steps to configure
this module are as follows.

 The Gateway service in DataTorrent RTS 2.0 is compatible with Jetty
    8. The class name identifying the role principal is
    “org.eclipse.jetty.plus.jaas.JAASRole”. When using the Jetty LDAP
    login module a custom JAAS callback has to be handled by the caller.
    This has been implemented by DataTorrent in a callback handler. The
    class name for the callback handler implementation should be
    specified as a property. The property name is
    “dt.gateway.http.authentication.jaas.callback.class.name”. The class
    name of the callback handler is
    “com.datatorrent.contrib.security.jetty.JettyJAASCallbackHandler”.
    This property should be specified along with the class name
    identifying the role principal for the Jetty login module as
    described in the section above. The configuration file with all the
    JAAS properties including these looks as follows

```
<configuration>
...
  <property>
          <name>dt.gateway.http.authentication.type</name>
          <value>jaas</value>
  </property>
  <property>
         <name>dt.gateway.http.authentication.jaas.name</name>
         <value>ldap</value>
  </property>
  <property>  
        <name>dt.gateway.http.authentication.jaas.role.class.name</name>
         <value>org.eclipse.jetty.plus.jaas.JAASRole</value>
  </property>
  </property>
<name>
	dt.gateway.http.authentication.jaas.callback.class.name </name>            
<value>
	com.datatorrent.contrib.security.jetty.JettyJAASCallbackHandler
</value>
  </property>
...
</configuration>
```
Note that the JAAS callback handler property can be used to specify a
custom callback handler. The source for the Jetty custom callback
handler used above can be found here

JettyJAASCallbackHandler

 An issue was discovered with the Jetty login module supplied with
    Jetty 8 that prevented LDAP authentication to be successful even
    when the user credentials were correct. DataTorrent has a fix for
    this and is providing the login module with the fix in a separate
    package called dt-auth. The classname for the module is
    “com.datatorrent.auth.jetty.JettyLdapLoginModule”. The dt-auth
    project along with the source can be found here
    [Auth](https://github.com/DataTorrent/Auth). DataTorrent
    is working on submitting this fix back to Jetty project so that it
    gets back into the main source.

The JAAS configuration file as described in
[LDAP](#h.ki9ds3jmagv) section under [Enabling JAAS
Auth](#JAAS) should be configured to specify the ldap settings
for roles. A sample configuration  roles based parameters to the
configuration shown before 

ldap {
   com.datatorrent.auth.jetty.JettyLdapLoginModule required
hostname="ldap-server-hostname" authenticationMethod="simple"
userBaseDn="ou=users,dc=domain,dc=com" userIdAttribute="uid"
userRdnAttribute="uid" roleBaseDn="ou=groups,dc=domain,dc=com"
roleNameAttribute=”cn”
contextFactory=”com.sun.jndi.ldap.LdapCtxFactory”;

};

For more ldap settings refer to the java documentation of the login
module.

1.  After the above configuration changes are made the gateway service
    needs to be restarted. Before restarting however the different
    classes specified in the configuration files above namely the
    DataTorrent Jetty callback handler, the Jetty login module with the
    DataTorrent fix and the Jetty dependencies containing the role class
    should all be available for Gateway. This can be done by specifying
    the jars containing these classes in a classpath variable that is
    used by Gateway.

        The jars can be obtained from the following project

[https://github.com/DataTorrent/Auth](https://github.com/DataTorrent/Auth)

Please follow the instructions in the above url to obtain the project
jar files. After obtaining the jar files perform the following step to
make them available to Gateway

 Edit the custom-env.sh configuration file, typically located under
    `/opt/datatorrent/current/conf` ( or `~/datatorrent/current/conf` for
    local install) and append the list of jars obtained above to the
    `DT\_CLASSPATH` variable. This needs to be added at the end of the
    file in the section for specifying local overrides to environment
    variables. The line would look like

     `export DT\_CLASSPATH=\${DT\_CLASSPATH}:path/to/jar1:path/to/jar2:..`

 Restart the Gateway by running

`sudo service dtgateway restart`

( when running Gateway in local mode use  dtgateway restart command)

<a name="ExternalRoleMapping"></a> External Role Mapping
---------------------

External role mapping is specified to map the external roles to the
DataTorrent roles. For example users from an LDAP group called admins
should have the admin role when using the Console. This can be specified
by doing the following steps

  In the configuration folder typically located under
    `/opt/datatorrent/current/conf` ( or `~/datatorrent/current/conf` for
    local install) edit the file called external-roles or create the
    file if it not already present. In this file each line contains a
    mapping from an external role to a datatorrent role separated by a
    delimiter ‘:’ An example listing is
`
        admins:admin
        staff: developer`

This maps the external role admins to the DataTorrent role admin and
external role staff to the DataTorrent role developer.

 Restart the Gateway by running

`sudo service dtgateway restart`

( when running Gateway in local mode use  dtgateway restart command)

Administering Using Command Line
================================

You can also utilize the [Gateway REST
API](https://www.datatorrent.com/docs/guides/DTGatewayAPISpecification.html) (under
/ws/v2/auth) to add or remove users and to change roles and passwords.
 Here are the examples with password authentication.

Log in as admin:
----------------

% curl -c \~/cookie-jar -XPOST -H "Content-Type: application/json" -d
'{"userName":"admin","password":"admin"}'
http://localhost:9090/ws/v2/login

This curl command logs in as user “admin” (with the default password
“admin”) and stores the cookie in \~/cookie-jar

Changing the admin password:
----------------------------

% curl -b \~/cookie-jar -XPOST -H "Content-Type: application/json" -d
'{"newPassword":"xyz"}' http://localhost:9090/ws/v2/auth/users/admin

This uses the “admin” credential from the cookie jar to change the
password to “xyz” for user “admin”.

Adding a second admin user:
---------------------------

% curl -b \~/cookie-jar -XPUT -H "Content-Type: application/json" -d
'{"password":"abc","roles": [ "admin" ] }'
http://localhost:9090/ws/v2/auth/users/john

This command adds a user “john” with password “abc” with admin access.

Adding a user in the developer role:
------------------------------------

```% curl -b \~/cookie-jar -XPUT -H "Content-Type: application/json" -d
'{"password":"abc","roles": ["developer"] (http://localhost:9090/ws/v1/login) }'
http://localhost:9090/ws/v2/auth/users/jane ```

This command adds a user “jane” with password “abc” with the developer
role.

Listing all users:
------------------

% curl -b \~/cookie-jar http://localhost:9090/ws/v2/auth/users

Getting info for a specific user:
---------------------------------

% curl -b \~/cookie-jar http://localhost:9090/ws/v2/auth/users/john

This command returns the information about the user “john”.

Removing a user:
----------------

% curl -b \~/cookie-jar -XDELETE
http://localhost:9090/ws/v2/auth/users/jane

This command removes the user “jane”.

Enabling HTTPS in Gateway
=========================

HTTPS in the Gateway can be enabled by performing following two steps.

Generate an SSL keystore if you don’t have one.  Instruction on how
    to generate an SSL keystore is here:
    [http://docs.oracle.com/cd/E19509-01/820-3503/ggfen/index.html](http://docs.oracle.com/cd/E19509-01/820-3503/ggfen/index.html)

Add a property to dt-site.xml configuration file, typically located
    under `/opt/datatorrent/current/conf` ( or
    `~/datatorrent/current/conf` for local install).

```
<configuration>
...
  <property>
           <name>dt.gateway.sslKeystorePath</name>
           <value>{/path/to/keystore}</value>
  </property>
  <property>
            <name>dt.gateway.sslKeystorePassword</name>
             <value>{keystore-password}</value>
  </property>
  <property> 
    <name>dt.attr.GATEWAY_USE_SSL</name>
          <value>true</value>
  </property>
...
\</configuration\>
```

Restart the Gateway by running

sudo service dtgateway restart

( when running Gateway in local mode use `dtgateway restart` command)

© 2013-2015 DataTorrent Inc.

