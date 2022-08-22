## Execution Sequence
***

### [NameNodeReplicator](namenode-agent/src/main/java/ai/sapper/hcdc/agents/main/NameNodeReplicator.java):
#### Copies the current state (as per the FSImage file) of the NameNode file system. 

[Configuration Sample](namenode-agent/src/test/resources/configs/namenode-agent.xml)

- Arguments:
  - JVM: 
  
    `-Dlog4j.configurationFile=<log4j Config Path>`
  - Program: 
  
    `--image <HDFS FSImage File (Path)> --config <Config File Path> [--type <Config File Type>(File, Remote, Resource)] [--tmp <Temp directory path>]`
- Queue(s)
  - None

### [DomainFilterLoader](namenode-agent/src/main/java/ai/sapper/hcdc/utils/DomainFilterLoader.java):
#### Registers (bootstrap) the entity filters on startup.

[Configuration Sample](namenode-agent/src/test/resources/configs/hcdc-agent.xml)

- Arguments:
  - JVM: 
  
    `-Dlog4j.configurationFile=<log4j Config Path>`
  - Program: 
  
    `--config <Config File Path> --filters <Path to the Filter definitions (CSV)`


- Queue(s):
  - None
  
### [Snapshot Service](services/src/main/java/ai/sapper/hcdc/services/namenode/SnapshotService.java):
#### SpringBoot services for running initial snapshot and adding/updating entity filters
- Services:
  - **{Method=POST}** /admin/snapshot/start :  Start the Snapshot service
  
        {
          "path" : "https://raw.githubusercontent.com/subhagho/hcdc/1.2-SNAPSHOT/change-manager/namenode-agent/src/test/resources/configs/snapshot.xml",
          "type" : "Remote"
        }
  - **{Method=POST}** /snapshot/run : Run the initial snapshot


- Queue(s):
  - `<namespace>.cdc` - **_Write_**

### [EditsLogProcessor](namenode-agent/src/main/java/ai/sapper/hcdc/agents/main/EditsLogProcessor.java):
#### Start the processor to read the HDFS Edits Log(s)

[Configuration Sample](namenode-agent/src/test/resources/configs/namenode-agent.xml)

- Arguments:
    - JVM: 

      `-Dlog4j.configurationFile=<log4j Config Path>`
    - Program: 

      `--config <Config File Path> [--type <Config File Type>(File, Remote, Resource)]`


- Queue(s):
  - `<namespace>.edits` - **_Write_**

### [EditsChangeConsumer](namenode-agent/src/main/java/ai/sapper/hcdc/agents/main/EditsChangeConsumer.java):
#### Process the parsed edits and sync the ZooKeeper file structure with NameNode. This process also uses the 
#### registered entities to filter edit changes that should be processed.

[Configuration Sample](namenode-agent/src/test/resources/configs/hcdc-agent.xml)


- Arguments:
  - JVM:

    `-Dlog4j.configurationFile=<log4j Config Path>`
  - Program:

    `--config <Config File Path> [--type <Config File Type>(File, Remote, Resource)]`


- Queue(s):
  - `<namespace>.edits` - **_Read_**
  - `<namespace>.cdc` - **_Write_**
  - `<namespace>.errors` - **_Write_**

### [EntityChangeDeltaConsumer](namenode-agent/src/main/java/ai/sapper/hcdc/agents/main/EntityChangeDeltaConsumer.java):
#### Processes the edits for entities registered for sync.

[Configuration Sample](namenode-agent/src/test/resources/configs/delta-agent.xml)


- Arguments:
  - JVM:

    `-Dlog4j.configurationFile=<log4j Config Path>`
  - Program:

    `--config <Config File Path> [--type <Config File Type>(File, Remote, Resource)]`


- Queue(s):
  - `<namespace>.cdc` - **_Read_**
  - `<namespace>.delta` - **_Write_**
  - `<namespace>.errors.delta` - **_Write_**




