#!/bin/bash

function pick_one
{
  FILES=$(ls $1 2>/dev/null)

  i=0
  TEXT="0. None\n"
  for file in $FILES
  do
    i=$[ i + 1]
    TEXT="$TEXT$i. $file\n";
  done

  FILE=""
  while test -z $FILE
  do
    case $i in
      0) echo No suitable files to pick\!
	 exit 2
	 ;;
      1) FILE=$FILES
	 ;;
      *) echo -ne $TEXT
	 echo -n "Pick a file? "
	 read res
	 if test "$res" -le "$i"
	 then
	   for file in $FILES
	   do
	     if test "$res" -eq "1"
	     then
		FILE=$file
		break
	     fi
	     res=$[ res - 1]
	   done
	 fi
    esac
  done
  eval "$2=$FILE"
}

HADOOP_CLASSPATH=$HADOOP_CLASSPATH:~/.m2/repository/com/malhartech/bufferserver/0.1-SNAPSHOT/bufferserver-0.1-SNAPSHOT.jar
HADOOP_CLASSPATH=$HADOOP_CLASSPATH:~/.m2/repository/com/malhar/app/stram/0.1-SNAPSHOT/stram-0.1-SNAPSHOT.jar

#kryo dependencies
HADOOP_CLASSPATH=$HADOOP_CLASSPATH:~/.m2/repository/com/esotericsoftware/kryo/kryo/2.16/kryo-2.16.jar:~/.m2/repository/com/esotericsoftware/reflectasm/reflectasm/1.06/reflectasm-1.06-shaded.jar
HADOOP_CLASSPATH=$HADOOP_CLASSPATH:~/.m2/repository/org/mockito/mockito-all/1.8.5/mockito-all-1.8.5.jar
HADOOP_CLASSPATH=$HADOOP_CLASSPATH:~/.m2/repository/com/esotericsoftware/minlog/minlog/1.2/minlog-1.2.jar

#twitter4j dependencies
HADOOP_CLASSPATH=$HADOOP_CLASSPATH:~/.m2/repository/org/twitter4j/twitter4j-core/2.2.5/twitter4j-core-2.2.5.jar
HADOOP_CLASSPATH=$HADOOP_CLASSPATH:~/.m2/repository/org/twitter4j/twitter4j-stream/2.2.5/twitter4j-stream-2.2.5.jar

export HADOOP_CLIENT_OPTS="$HADOOP_OPTS"

PROJECT=$1
JAR="$PROJECT/target/*.jar"
pick_one "$JAR" JAR
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$JAR

TPLG="$PROJECT/src/main/resources/*.tplg.properties $PROJECT/src/test/resources/*.tplg.properties"
pick_one "$TPLG" TPLG

$HADOOP_PREFIX/bin/hadoop com.malhartech.stram.StramClient --debug --container_memory 64 --master_memory 256 --num_containers 2 --topologyProperties "$TPLG"
