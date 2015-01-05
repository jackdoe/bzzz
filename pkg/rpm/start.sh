#!/bin/sh
if [ $# != 1 ]; then
    echo "Usage: $0 bzzz-0.config"
    exit 1
fi
BASE=`basename $1`

#can overwrite everything
source /etc/bzzz/$BASE

########
# from elasticsearch.in.sh
if [ -x "$JAVA_HOME/bin/java" ]; then
    JAVA="$JAVA_HOME/bin/java"
else
    JAVA=`which java`
fi
JAVA_OPTS="$JAVA_OPTS -XX:+PrintGCDetails"

if [ "x$BZZZ_OPTS" = "x" ]; then
    BZZZ_OPTS=""
fi

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

if [ "x$BZZZ_IDENTIFIER" = "x" ]; then
    echo "need to setup BZZZ_IDENTIFIER"
    exit 1
fi

if [ "x$BZZZ_PORT" = "x" ]; then
    echo "need to setup BZZZ_PORT"
    exit 1
fi

if [ "x$BZZZ_DIRECTORY" = "x" ]; then
    echo "need to setup BZZZ_DIRECTORY"
    exit 1
fi
$JAVA $JAVA_OPTS \
      -jar /usr/lib/bzzz/bzzz.jar \
      --directory $BZZZ_DIRECTORY \
      --port $BZZZ_PORT \
      --identifier $BZZZ_IDENTIFIER $BZZZ_OPTS
