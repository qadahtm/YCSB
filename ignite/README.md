<!--
Copyright (c) 2012 - 2015 YCSB contributors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

## Quick Start

This section describes how to run YCSB on Apache Ignite. 

### 1. Install Java and Maven

Go to http://www.oracle.com/technetwork/java/javase/downloads/index.html

and get the url to download the rpm into your server. For example:

    wget http://download.oracle.com/otn-pub/java/jdk/7u40-b43/jdk-7u40-linux-x64.rpm?AuthParam=11232426132 -o jdk-7u40-linux-x64.rpm
    rpm -Uvh jdk-7u40-linux-x64.rpm
    
Or install via yum/apt-get

    sudo yum install java-devel

Download MVN from http://maven.apache.org/download.cgi

    wget http://ftp.heanet.ie/mirrors/www.apache.org/dist/maven/maven-3/3.1.1/binaries/apache-maven-3.1.1-bin.tar.gz
    sudo tar xzf apache-maven-*-bin.tar.gz -C /usr/local
    cd /usr/local
    sudo ln -s apache-maven-* maven
    sudo vi /etc/profile.d/maven.sh

Add the following to `maven.sh`

    export M2_HOME=/usr/local/maven
    export PATH=${M2_HOME}/bin:${PATH}

Reload bash and test mvn

    bash
    mvn -version

### 2. Start Apache Ignite

See https://apacheignite.readme.io/docs on how to deply Ignite. Sample configuration files can be found under the `config` folder

Alternativly, you can start an ignite node using the ``` ServerNode``` and use the following command:
```
$ java -cp "./ignite/target/dependency/*:./ignite/target/ignite-binding-0.7.0-SNAPSHOT.jar" com.yahoo.ycsb.examples.ignite.ServerNode -t
```

### 3. Set Up YCSB

Clone the repo and build using the following command:

```
$ mvn clean package -DskipTests
```

### 4. Run YCSB

You can run Ignite in two modes: `Transactional` and `Atomic`. You can use the parameter `ignite.txmode=true` to run the client in `Transactional` mode. The following command will run YCSB client with 8 threads using the ce_workload. 

```
./bin/ycsb load ignite -P workloads/ce_workload -p ignite.cachename=testing -p ignite.txmode=true -threads 8 -p ignite.conffile=./ignite/config/example-ignite.xml
```

## Ignite Configuration Parameters

- `ignite.conffile`
  - This should be a path to the configuration file for Ignite.

- `ignite.txmode`
  - set to `true` to run in transactional mode

- `ignite.cachename`
  - The name for the cache to be used by the Ignite client.
