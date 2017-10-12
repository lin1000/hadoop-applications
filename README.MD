
# My Hadoop Applications Templates

Classic .. WordCount Example
```shell
hadoop jar target/hadoop-applications-1.0.jar assets.a01.WordCount /home/tony/hadoop-applications/a01/clusterFile /home/tony/hadoop-applications/a01/output{N}
```

Classic .. WordCount2 Example
```shell
hadoop jar target/hadoop-applications-1.0.jar assets.a02.WordCount2 /home/tony/hadoop-applications/a02/clusterFile /home/tony/hadoop-applications/a02/output{N}
```

Parsing API endpont logs : NginxLogParser
```shell
hdfs dfs -copyFromLocal src/main/resources/nginxlog /home/tony/hadoop-applications/a03/input
hadoop jar target/hadoop-applications-1.0.jar assets.a03.NginxLogParser /home/tony/hadoop-applications/a03/input/nginxlog/access.*.gz /home/tony/hadoop-applications/a03/output20
```
```txt
output look liks
10.XXX.XXX.XXX	361
10.XXX.XXX.OOO	3
10.XXX.XXX.YYY	3
```