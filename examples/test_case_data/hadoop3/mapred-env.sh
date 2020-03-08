
      # export JAVA_HOME=/home/y/libexec/jdk1.6.0/

      export HADOOP_JOB_HISTORYSERVER_HEAPSIZE=900

      # We need to add the RFA appender for the mr daemons only;
      # however, HADOOP_MAPRED_LOGGER is shared by the mapred client and the
      # daemons. This will restrict the RFA appender to daemons only.
      INVOKER="${0##*/}"
      if [ "$INVOKER" == "mr-jobhistory-daemon.sh" ]; then
        export HADOOP_MAPRED_ROOT_LOGGER=${HADOOP_MAPRED_ROOT_LOGGER:-INFO,RFA}
      else
        export HADOOP_MAPRED_ROOT_LOGGER=INFO,console
      fi

      
      #export HADOOP_MAPRED_LOG_DIR="" # Where log files are stored.  $HADOOP_MAPRED_HOME/logs by default.
      #export HADOOP_JHS_LOGGER=INFO,RFA # Hadoop JobSummary logger.
      #export HADOOP_MAPRED_PID_DIR= # The pid files are stored. /tmp by default.
      #export HADOOP_MAPRED_IDENT_STRING= #A string representing this instance of hadoop. $USER by default
      #export HADOOP_MAPRED_NICENESS= #The scheduling priority for daemons. Defaults to 0.
      export HADOOP_OPTS="-Dhdp.version=$HDP_VERSION $HADOOP_OPTS"
      export HADOOP_OPTS="-Djava.io.tmpdir=/var/lib/ambari-server/data/tmp/hadoop_java_io_tmpdir $HADOOP_OPTS"
      export JAVA_LIBRARY_PATH="${JAVA_LIBRARY_PATH}:/var/lib/ambari-server/data/tmp/hadoop_java_io_tmpdir"