#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

. /lib/lsb/init-functions

set -e

echo -n "* Starting hadoop services. This might take few seconds."
echo -n "."; sudo /etc/init.d/hadoop-hdfs-namenode restart >/dev/null \
  && echo -n "."; sudo /etc/init.d/hadoop-hdfs-datanode restart >/dev/null \
  && echo -n "."; sudo /etc/init.d/hadoop-yarn-resourcemanager restart >/dev/null \
  && echo -n "."; sudo /etc/init.d/hadoop-yarn-nodemanager restart >/dev/null \
  && echo -n "."; sudo /etc/init.d/hadoop-yarn-timelineserver restart >/dev/null \
  && echo -n "."; sudo /etc/init.d/ssh start >/dev/null
log_end_msg $?

echo
echo "====================================="
echo " Welcome to Apache Apex Test Sandbox "
echo "====================================="
echo "This docker image uses bigtop package of hadoop and apex."
echo "This image provides a ready to use environment for quickly launching apex application."
echo "Currently running docker container has hadoop services initialized and started."
echo 
echo "Just type \"apex\" on command line to get apex cli console. See man page of apex for details."
echo "Enjoy Apexing!!!"
echo
echo "=====Information about Container====="
echo "IPv4 Address: $(hostname -i)"
echo "Hostname: $(hostname)"
echo

/bin/bash
