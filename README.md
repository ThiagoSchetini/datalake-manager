# Datalake Manager
First, notice the "SM" or "sm" on the entire system. It means: Smart Contract

... to continue with mnore instructions

### requirements
Up and running non kerberos environment:

    - UNIX
    - JDK 8 Update 221
    - Maven 3+
    - Hadoop 2.6.0
    - Hive 1.1.0
    - HiveServer2 1.1.0
    - Spark 2.1.0

### how to config:
Open src/main/resources/core.properties

Change to a valid hadoop configuration directory:

`hadoop.conf.dir=/your/hadoop/env/config`

Change with your OS username:

`hiveserver2.url=jdbc:hive2://localhost:10000/;user=username`

### how to run:
From the root folder, run: 

`sh execute.sh`

### Tuning JVM for production:
Use this flags on JVM to optimize young generation memory and make it elastic to OS:
```
-XX:+UseG1GC 
-Xmx8G 
-XX:NewRatio=1 
-XX:SurvivorRatio=128 
-XX:MinHeapFreeRatio=5 
-XX:MaxHeapFreeRatio=5 
```

### how to debug on InteliJ:
Open "Run/Debug Configurations":

`Add New Configuration: Application`

`Main Class: br.com.bvs.datalake.core.Initializer`

`VM options: -XX:+UseG1GC -Xmx8G -XX:NewRatio=1 -XX:SurvivorRatio=128 -XX:MinHeapFreeRatio=5 -XX:MaxHeapFreeRatio=5`

`Environment variables: DATALAKE_MANAGER_PROPS=src/main/resources`

### references
Cassandra: `https://medium.com/rahasak/scala-cake-pattern-e0cd894dae4e`

Docker Compose: `https://carledwinti.wordpress.com/2019/11/02/instalar-o-docker-no-ubuntu-19-10/`

HiveServer2: `https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients#HiveServer2Clients-JDBC`

OS Process with Scala: 

    https://www.scala-lang.org/api/2.12.4/scala/sys/process/index.html
    https://www.scala-lang.org/api/2.12.4/scala/sys/process/ProcessBuilder.html