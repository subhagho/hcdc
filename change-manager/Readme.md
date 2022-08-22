***
## **_Execution Sequence_**
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
  - **_None_**

### [DomainFilterLoader](namenode-agent/src/main/java/ai/sapper/hcdc/utils/DomainFilterLoader.java):
#### Registers (bootstrap) the entity filters on startup.

[Configuration Sample](namenode-agent/src/test/resources/configs/hcdc-agent.xml)

- Arguments:
  - JVM: 
  
    `-Dlog4j.configurationFile=<log4j Config Path>`
  - Program: 
  
    `--config <Config File Path> --filters <Path to the Filter definitions (CSV)`


- Queue(s):
  - **_None_**
  
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

### [EntityChangeDeltaRunner](namenode-agent/src/main/java/ai/sapper/hcdc/agents/main/EntityChangeDeltaRunner.java):
#### Reads the edit changes and fetches the required change deltas from HDFS. Creates a change delta set for downstream.
###### _Note:_ Multiple instances of this process can be run, based on the number of partitions specified in the input queue.

[Configuration Sample](namenode-agent/src/test/resources/configs/file-delta-agent-0.xml)


- Arguments:
  - JVM:

    `-Dlog4j.configurationFile=<log4j Config Path>`
  - Program:

    `--config <Config File Path> [--type <Config File Type>(File, Remote, Resource)]`


- Queue(s):
  - `<namespace>.delta` - **_Read_**
  - `<namespace>.delta.files` - **_Write_**
  - `<namespace>.errors.files` - **_Write_**

***
**_OPTIONAL_**
***

### [SchemaScanner](namenode-agent/src/main/java/ai/sapper/hcdc/agents/main/SchemaScanner.java):
#### Crawls the HDFS files system to extract schema (AVRO) from supported file types.

[Configuration Sample](namenode-agent/src/test/resources/configs/hdfs-files-scanner.xml)


- Arguments:
  - JVM:

    `-Dlog4j.configurationFile=<log4j Config Path>`
  - Program:

    `--config <Config File Path> [--type <Config File Type>(File, Remote, Resource)]`


- Queue(s):
  - **_None_**

***
#### **_Configuration Template_**
***
    
    <configuration>
        <agent>
            <module>[module name]]</module>
            <instance>[process name]</instance>
            <source>[namespace]</source>
            <enableHeartbeat>true/false</enableHeartbeat>
            ...
        </agent>
        <locks>
            <lock>
                <name>[name]</name>
                <connection>[zookeeper connection name]</connection>
                <lock-node>[ZK Lock Path]</lock-node>
            </lock>
            ...
        </locks>
        <config>
            <connections>
                <connection>
                    <type>[connection class]</type>
                    ...
                </connection>
                ...
            </connections>
        </config>
    </configuration>
    




