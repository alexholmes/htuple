#! /usr/bin/env bash
#############################################################################
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
#############################################################################
#
# run-example.sh:  Launch the secondary sorting example.
#
# Pre-requisites:
#
# 1) Your environment is setup such that the "hadoop" script is in
#    your path.  You can test this by running:
#
#    $ hadoop
#
#############################################################################

# Uncomment the line below if you want to debug this script.
# Alternatively run this script with "bash -x <script>"
#
# set -x

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
BASEDIR=`cd ${BASEDIR}/..;pwd`

# check command line args
if [[ $# != 2 ]]; then
  echo "usage: $(basename $0) <HDFS input dir> <HDFS output dir>"
  exit 1
fi

LIBJARS=""

function add_jars() {
  dir=$1
  for f in $dir/*.jar; do
    LIBJARS=${LIBJARS},$f
  done

  export LIBJARS
}

add_jars ${BASEDIR}

EXAMPLE_JAR=`find ${BASEDIR} -regex '.*htuple-examples-[0-9\.]+.jar'`

if [ "${EXAMPLE_JAR}" == "" ]; then
  echo "Oops, something went wrong and we couldn't find the example JAR :-("
  exit 2
fi

export HADOOP_CLASSPATH=`echo ${LIBJARS} | sed s/,/:/g`

hadoop jar ${EXAMPLE_JAR} org.htuple.examples.SecondarySort -libjars ${LIBJARS} "$@"
