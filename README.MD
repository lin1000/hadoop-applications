
# My Hadoop Applications Templates

Everyone knows .. WordCount
```
hadoop jar target/hadoop-applications-1.0.jar assets.a01.WordCount /home/tony/hadoop-applications/a01/clusterFile /home/tony/hadoop-applications/a01/output{N}
```

Almost Everyone knows .. WordCount2
```
hadoop jar target/hadoop-applications-1.0.jar assets.a02.WordCount2 /home/tony/hadoop-applications/a02/clusterFile /home/tony/hadoop-applications/a02/output{N}
```

Parsing API endpont logs : NginxLogParser
```
hdfs dfs -copyFromLocal src/main/resources/nginxlog /home/tony/hadoop-applications/a03/input
hadoop jar target/hadoop-applications-1.0.jar assets.a03.NginxLogParser /home/tony/hadoop-applications/a03/input/nginxlogs /home/tony/hadoop-applications/a03/output{N}
```
