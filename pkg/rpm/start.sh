#!/bin/sh
if [ $# != 1 ]; then
    echo "Usage: $0 bzzz-0.config"
    exit 1
fi
BASE=`basename $1`
########
# from elasticsearch.in.sh
if [ -x "$JAVA_HOME/bin/java" ]; then
    JAVA="$JAVA_HOME/bin/java"
else
    JAVA=`which java`
fi
JAVA_OPTS=""
JAVA_OPTS="$JAVA_OPTS -XX:+UseParNewGC"
JAVA_OPTS="$JAVA_OPTS -XX:+UseConcMarkSweepGC"
JAVA_OPTS="$JAVA_OPTS -XX:CMSInitiatingOccupancyFraction=75"
JAVA_OPTS="$JAVA_OPTS -XX:+UseCMSInitiatingOccupancyOnly"

if [ "x$BZZZ_MIN_MEM" = "x" ]; then
    BZZZ_MIN_MEM=256m
fi
if [ "x$BZZZ_MAX_MEM" = "x" ]; then
    BZZZ_MAX_MEM=512m
fi
if [ "x$BZZZ_HEAP_SIZE" != "x" ]; then
    BZZZ_MIN_MEM=$BZZZ_HEAP_SIZE
    BZZZ_MAX_MEM=$BZZZ_HEAP_SIZE
fi
JAVA_OPTS="$JAVA_OPTS -Xms${BZZZ_MIN_MEM}"
JAVA_OPTS="$JAVA_OPTS -Xmx${BZZZ_MAX_MEM}"
########

if [ ! -x "$JAVA" ]; then
    echo "Could not find any executable java binary. Please install java in your PATH or set JAVA_HOME"
    exit 1
fi

source /etc/bzzz/$BASE
exec 0>&-
exec 1>&-
exec 2>&-
$JAVA -Dlog4j.configuration=file:/etc/bzzz/log4j.properties $JAVA_OPTS \
      -jar /usr/lib/bzzz/bzzz.jar \
      --directory $BZZZ_DIRECTORY \
      --port $BZZZ_PORT \
      --hosts $BZZZ_HOSTS \
      --identifier $BZZZ_IDENTIFIER
