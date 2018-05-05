#### [Apache Apex](http://apex.apache.org/) test sandbox
This repository contains a ready to use [Apache Apex](http://apex.apache.org/) test sandbox docker image in which hadoop and apex is preinstalled and running. All one need to do is launch this docker image and start playing with apex.
#### Basic
##### Run apex docker container
This will create and start a docker container from the docker image.
```
docker run -it --name=apex-sandbox apacheapex/sandbox
```
##### Start apex cli after launching docker container
```
apex@62550653e2d8:~$ apex
Apex CLI 3.5.0 15.06.2016 @ 08:20:44 UTC rev: 85a2bdd branch: 85a2bdd9bfce49a904b45a4d0d015434d1a89216
apex> 
```
#### Advanced
##### Linux user information
Username: apex

Password: apex

**NOTE**: User "apex" is added in sudoers list and is provided with root privileges.

##### Run docker container with host directory mounted
This will create and start a docker container and docker images while mounting local filesystem directory as a mount point inside docker container.
```
docker run -it --name=apex-sandbox -v /local/path/to/mount:/mount_location apacheapex/sandbox
```
##### Start already created docker container
```
docker start -i apex-sandbox
```
##### Hadoop and YARN WebUI from local machine
Following command will map yarn and hadoop ports exposed inside docker container to be mapped to ports of host machine.
```
docker run -it --name=apex-sandbox -p 50070:50070 -p 8088:8088 apacheapex/sandbox
```
After docker has started, one can point host machine's browser to *localhost:50070* and *localhost:8088* to see hadoop and yarn WebUI respectively.
