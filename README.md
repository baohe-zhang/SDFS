# SimpleDFS

SimpleDFS, Simple Distributed File System, CS 425 MP3, implemented by Grp#29 Baohe Zhang & Kechen Lu. It implemented a HDFS-like distributed file system, which includes the versioned file control, fast file replication mechanism, master node re-election and data node re-replication after failures, metadata failure handling, and leverages the membership service and failure detector implemented at last MP2.

## Project Info

- Language: Golang
- Tested Platform: macOS 10.13, CentOS 7
- Code file structure
  - /client: Client which contact with the master node
  - /daemon: Main node server side daemon process for data node or/and master node
  - /datanode: Data node package handling replication pipelining and re-replication
  - /election: Master node re-election implementation after failures
  - /master: Master node handling client request as a coordinator 
  - /membership: Membership service and failure detector
  - /scripts: Helper scripts for updating git repo or start/stop process
  - /utils: Utilities package for our SimpleDFS

## How-to

### Build and Deploy

To build the SimpleDFS project, we will go to the `client` directory and `daemon` directory to build separately. Just run

```shell
$ cd ./client
$ go build
$ cd ../daemon
$ go build
```

To deploy on the each machine of the cluster, we have to git clone this repo like below. To simply the repo update and build in the VM cluster, we have a easy-to-use script, each time we push a new commit to remote repo then run.

```shell
$ git clone git@gitlab.engr.illinois.edu:kechenl3/simpledfs.git
$ ./scripts/update_all.sh
```

### Run Daemon

To run our SimpleDFS daemon, go to the `./daemon/` directory. We can use `./daemon -h` to get command help

```shell
$ cd ./daemon
$ ./daemon -h
Usage of ./daemon:
  -dn-port int
    	DataNode serving port (default 5002)
  -master string
    	Master's IP (default "127.0.0.1")
  -mem-port int
    	Membership serving port (default 5001)
  -mn-port int
    	MasterNode serving port (default 5000)

```

For example we can start the daemon with VM1 and set its master(also as the introducer) to itself.

```shell
$ cd ./daemon
$ ./daemon -master fa18-cs425-g29-01.cs.illinois.edu
[INFO]: Start service
elector start
join
ls
------------------------------------------
Size: 5
idx: 0, TS: 1541390207480104000, IP: 10.194.16.24, ST: 101
idx: 1, TS: 1541389675448818039, IP: 172.22.154.96, ST: 101
idx: 2, TS: 1541388516767419985, IP: 172.22.156.95, ST: 1101
idx: 3, TS: 1541389702066352414, IP: 172.22.156.96, ST: 101
idx: 4, TS: 1541388532773448492, IP: 172.22.158.95, ST: 101
------------------------------------------
id
Member (1541390207480104000, 10.194.16.24)
```

User can do front-end interaction in terminal when running and all the info/debug/error level logs would be stored in **ssms.log** file. We have four interaction command in the console. 

1. `join`,  join in the group
2. `leave`, voluntarily leave the group 
3. `ls`, show membership list 
4. `id`, show the id of this process itself

### Run Client

To run our SimpleDFS client, go to the `./client/` directory. We can use `./client` to get command help

```shell
$ ./client 
Usage of ./client
   -master=[master IP:Port] put [localfilename] [sdfsfilename]
   -master=[master IP:Port] get [sdfsfilename] [localfilename]
   -master=[master IP:Port] delete [sdfsfilename]
   -master=[master IP:Port] ls [sdfsfilename]
   -master=[master IP:Port] store
   -master=[master IP:Port] get-versions [sdfsfilename] [num-versions] [localfilename]
```

####  put Command

We could use the command below to put a file "./client.go" to the SimpleDFS as the sdfsfile name "SSD".

```shell
$ ./client -master fa18-cs425-g29-01.cs.illinois.edu:5000 put ./client.go SSD
Master IP:Port address  fa18-cs425-g29-01.cs.illinois.edu:5000
[./client.go SSD]
11026 SSD
OIgc90WYPru9UYAx5k6nnBLBtLFArI8hs7_cODCKXfU= 1541390504417011742 [{1541389702066352414 2887162976} {1541388532773448492 2887163487} {1541390207480104000 180490264} {1541389675448818039 2887162464}]
OK
Send file finish
```

#### get Command

We could use the command below to get a sdfsfile "SSD" as local file name "./my.go".

```shell
$ ./client -master fa18-cs425-g29-01.cs.illinois.edu:5000 get SSD ./my.go
Master IP:Port address  fa18-cs425-g29-01.cs.illinois.edu:5000
[SSD ./my.go]
Dial  172.22.156.96  successful
Sent ReadRequest
Receive file with 11026 bytes
```

#### delete Command

We could use the command below to delete a sdfsfile "SSD"

```shell
$ ./client -master fa18-cs425-g29-01.cs.illinois.edu:5000 delete SSD
Master IP:Port address  fa18-cs425-g29-01.cs.illinois.edu:5000
[SSD]
SDFS File SSD successfully deleted
```

#### ls Command

We could use the command below to ls nodes IP addresses who keep a sdfsfile "SSD".

```shell
$ ./client -master fa18-cs425-g29-01.cs.illinois.edu:5000 ls SSD 
Master IP:Port address  fa18-cs425-g29-01.cs.illinois.edu:5000
[SSD]
SDFS File SSD stores in below addresses
172.22.156.96
172.22.158.95
10.194.16.24
172.22.154.96
```

#### store Command

We could use the command below to list sdfsfiles in this machine.

```shell
$ ./client -master fa18-cs425-g29-01.cs.illinois.edu:5000 store
Master IP:Port address  fa18-cs425-g29-01.cs.illinois.edu:5000
SSD
```

#### get-versions Command

We could use the command below to get all file versions of a sdfsfile "SSD" as local file starting with "Myfile", now can see 3 three version of SSD: `Myfile-v1541391020573826780` , `Myfile-v1541391024407933192`, `Myfile-v1541391032476226536` 

```shell
$ ./client -master fa18-cs425-g29-01.cs.illinois.edu:5000 get-versions SSD 5 ./Myfile
Master IP:Port address  fa18-cs425-g29-01.cs.illinois.edu:5000
[SSD 5 ./Myfile]
Dial  172.22.156.96  successful
Sent ReadVersionRequest
Receive versioned file with 3266 bytes
Dial  172.22.156.96  successful
Sent ReadVersionRequest
Receive versioned file with 3138864 bytes
Dial  172.22.156.96  successful
Sent ReadVersionRequest
Receive versioned file with 11026 bytes
wirelessprv-10-194-16-24:client colearolu$ ls
Myfile-v1541391020573826780	client
Myfile-v1541391024407933192	client.go
Myfile-v1541391032476226536	my.go
```

