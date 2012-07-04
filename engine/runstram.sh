
DIR="$( cd -P "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
JAR=$DIR/target/stram-0.1-SNAPSHOT.jar
export HADOOP_CLASSPATH=$DIR/../bufferserver/target/*:$DIR/target/*
#export HADOOP_CLASSPATH=/home/hdev/devel/malhar/stramproto/bufferserver/target/bufferserver-0.1-SNAPSHOT.jar
ls $DIR/../bufferserver/target/*.jar
export HADOOP_CLIENT_OPTS="-Dstram.libjars=$DIR/target/stram-0.1-SNAPSHOT-tests.jar $HADOOP_OPTS"
$HADOOP_PREFIX/bin/hadoop jar $JAR com.malhartech.stram.StramClient --container_memory 64 --master_memory 256 --num_containers 2 --topologyProperties $DIR/src/test/resources/testTopology.properties

