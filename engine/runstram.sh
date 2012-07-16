
DIR="$( cd -P "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
JAR=$DIR/target/stram-0.1-SNAPSHOT.jar
HADOOP_CLASSPATH=$DIR/../bufferserver/target/*:$DIR/target/*

#kryo dependencies
HADOOP_CLASSPATH=$HADOOP_CLASSPATH:~/.m2/repository/com/esotericsoftware/kryo/kryo/2.16/kryo-2.16.jar:~/.m2/repository/com/esotericsoftware/reflectasm/reflectasm/1.06/reflectasm-1.06-shaded.jar
HADOOP_CLASSPATH=$HADOOP_CLASSPATH:~/.m2/repository/org/mockito/mockito-all/1.8.5/mockito-all-1.8.5.jar
HADOOP_CLASSPATH=$HADOOP_CLASSPATH:~/.m2/repository/com/esotericsoftware/minlog/minlog/1.2/minlog-1.2.jar
export HADOOP_CLASSPATH

export HADOOP_CLIENT_OPTS="-Dstram.libjars=$DIR/target/stram-0.1-SNAPSHOT-tests.jar $HADOOP_OPTS"
$HADOOP_PREFIX/bin/hadoop jar $JAR com.malhartech.stram.StramClient --debug --container_memory 64 --master_memory 256 --num_containers 2 --topologyProperties $DIR/src/main/java/com/malhartech/example/wordcount/topology.properties

