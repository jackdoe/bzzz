#! /bin/bash
### BEGIN INIT INFO
# Provides:          bzzz
# Required-Start:    $all
# Required-Stop:     $all
# Default-Start:
# Default-Stop:      0 1 6
# Short-Description: Starts bzzz
# chkconfig: - 80 15
# Description: bzzz lucene network wrapper
### END INIT INFO

. /etc/rc.d/init.d/functions
[ -f /etc/sysconfig/bzzz ] && . /etc/sysconfig/bzzz
[ -z "$JAVA_HOME" -a -x /etc/profile.d/java.sh ] && . /etc/profile.d/java.sh
JAVA="java"
NAME=bzzz
NFILES=${NFILES:-32768}
DAEMON="/usr/lib/bzzz/start.sh"

export BZZZ_USER="bzzz"
start() {
    ulimit -n $NFILES
    for i in `ls -1 /etc/bzzz/bzzz-*.config`; do
        echo -n "Starting $i"
        runuser -s /bin/bash $BZZZ_USER -c "/usr/lib/bzzz/start.sh $i 2>&1 | logger -t 'bzzz unhandled exception' &"
        rc=$?
        if [ $rc -ne 0 ]; then
            echo "... failed"
            return $rc
        else
            echo "... done"
        fi
    done
    return 0
}

stop() {
    echo -n $"Stopping _all_ bzzz instances: pkill -f 'java.*/usr/lib/bzzz/bzzz.jar'"
    pkill -f 'java.*/usr/lib/bzzz/bzzz.jar'
    echo
    return 0
}

case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    status)
        pgrep -f 'java.*/usr/lib/bzzz/bzzz.jar'
        RETVAL=$?
        ;;
    restart|reload|force-reload)
        stop
        sleep 1
        start
        ;;
    *)
        N=/etc/init.d/${NAME}
        echo "Usage: $N {start|stop|status|restart|force-reload}" >&2
        RETVAL=2
        ;;
esac

exit $RETVAL
