# Datalake Manager
Reactive system build up on AKKA actors system. Schedule, invoke and monitor business tasks on Big Data cluster!

Warning: Clone on ~/repository/datalake 

"SM"/"sm" signum = Smart Contract

### TODO 

For the first deploy:
    
    + PT-BR types
    + PT-BR transactions
    + sm validation
    
For future versions:

    + Fields will be optional (choose)
    + cobol/positional in
    + tunning on YARN

### requirements
Up and running environment:

    - UNIX
    - JDK 8 Update 221
    - Maven 3+
    - Hadoop 2.6.0 
    - Yarn 2.6.0 (with default queue set)
    - Hive 1.1.0
    - HiveServer2 1.1.0
    - Spark 2.1.0

datalake-spark project required:

    - clone datalake-spark inside ~/repository/datalake 
    - "mvn package" inside datalake-spark root folder

### check the core.properties:
Open src/main/resources/core.properties

Change to a valid hadoop configuration directory:

`hadoop.conf.dir=/your/hadoop/env/config`

Change the username to your OS username on this connection string:

`hiveserver2.url=jdbc:hive2://localhost:10000/;user=username`

### how to run:
From the root folder, run: 

`mvn package`

Them:

`sh start-manager.sh`

To Shut Down:

`touch target/shutdown-signal/shutdown.signal`

### how to debug on InteliJ:
Considering your cloned on "~/repository/datalake" Open "Run/Debug Configurations":

Add New Configuration: `Application`

Main Class: `br.com.bvs.datalake.core.Initializer`

VM options: `-XX:+UseG1GC -Xmx8G -XX:NewRatio=1 -XX:SurvivorRatio=128 -XX:MinHeapFreeRatio=5 -XX:MaxHeapFreeRatio=5`

Environment variables: 

    DATALAKE_MANAGER_PROPS=src/main/resources
    DATALAKE_SPARK_PROPS=../datalake-spark/src/main/resources
    DATALAKE_SPARK_JARS=../datalake-spark/target
    
Before launch: 

    check build is added
    add: Run Maven Goal/test

### references
Cassandra: `https://medium.com/rahasak/scala-cake-pattern-e0cd894dae4e`

Docker Compose: `https://carledwinti.wordpress.com/2019/11/02/instalar-o-docker-no-ubuntu-19-10/`

HiveServer2: `https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients#HiveServer2Clients-JDBC`

OS Process with Scala: 

    https://www.scala-lang.org/api/2.12.4/scala/sys/process/index.html
    https://www.scala-lang.org/api/2.12.4/scala/sys/process/ProcessBuilder.html