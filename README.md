# Datalake Manager

...
# TODO implementar com Cassandra https://medium.com/rahasak/scala-cake-pattern-e0cd894dae4e


### requirements

- UNIX to install and run
- Java 8 or +
- Hadoop

### how to config, package and run with tests + integration test:

Warning: you are advised that there WILL be forced exceptions on integration test

On src/main/resources/app.properties, put a valid hadoop configuration directory:
```
HADOOP_CONF_DIR=/your/hadoop/env/config
```


Finally, from the root folder, run: 
```
mvn integration-test
```

### how to run:

Put some xml files on your home integration test path: ~/test/datalakemanager

Run the production shell on your test folder: 
```
sh ~/test/datalakemanager/execute.sh
```

### Tuning JVM for production:

Use this flags on JVM to optimize young generation memory:
```
-XX:+UseG1GC 
-Xmx24G 
-XX:NewRatio=1 
-XX:SurvivorRatio=128 
-XX:MinHeapFreeRatio=5 
-XX:MaxHeapFreeRatio=5 
```

### how to debug on InteliJ:

Put some xml files on your home integration test path: ~/test/datalakemanager, then

- Open "Run/Debug Configurations":

- Add New Configuration: Application

- Main Class: br.com.bvs.datalake.core.Initializer

- VM options (optional for big files): -XX:+UseG1GC -Xmx8G -XX:NewRatio=1 -XX:SurvivorRatio=128 -XX:MinHeapFreeRatio=5 -XX:MaxHeapFreeRatio=5

- Working directory: * click and choose where your datalake-manager repo is

- Environment variables: DATALAKE_MANAGER=/"home-path"/test/datalakemanager/properties