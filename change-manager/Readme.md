## Execution Sequence
***

### NameNodeReplicator:
#### Copies the current state (as per the FSImage file) of the NameNode file system.
- Arguments:
  - JVM: `-Dlog4j.configurationFile=<log4j Config Path>`
  - Program: `--image <HDFS FSImage File (Path)> --config <Config File Path (namenode-agent.xml)> [--tmp <Temp directory path>]`

### DomainFilterLoader:
#### Registers (bootstrap) the entity filters on startup.
- Arguments:
  - JVM: `-Dlog4j.configurationFile=<log4j Config Path>`
  - Program: `--config <Config File Path (namenode-agent.xml)> --filters <Path to the Filter definitions (CSV)`

### Snapshot Service:
#### SpringBoot services for running initial snapshot and adding/updating entity filters
- Services:
  - [POST]/admin/snapshot/start :  Start the Snapshot service
        {
          "path" : "https://raw.githubusercontent.com/subhagho/hcdc/1.2-SNAPSHOT/change-manager/namenode-agent/src/test/resources/configs/snapshot.xml",
          "type" : "Remote"
        }
  - [POST]/snapshot/run : Run the initial snapshot


