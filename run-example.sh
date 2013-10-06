

# check command line args
if [[ $# != 2 ]]; then
  echo "usage: $(basename $0) <HDFS input dir> <HDFS output dir>"
  exit 1;
fi

# resolve links - $0 may be a softlink
PRG="${0}"

while [ -h "${PRG}" ]; do
  ls=`ls -ld "${PRG}"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "${PRG}"`/"$link"
  fi
done

BASEDIR=`dirname ${PRG}`

CORE_JAR=${BASEDIR}/core/target/htuple-core-0.1.0.jar
EXAMPLE_JAR=${BASEDIR}/examples/target/htuple-examples-0.1.0.jar

export LIBJARS=${CORE_JAR},${EXAMPLE_JAR}
export HADOOP_CLASSPATH=${CORE_JAR}:${EXAMPLE_JAR}:${HADOOP_CLASSPATH}

hadoop jar ${EXAMPLE_JAR} org.htuple.examples.SecondarySort -libjars ${LIBJARS} "$@"
