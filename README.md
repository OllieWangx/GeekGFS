# GeekGFS

Simple implementation of **GFS**, refer to [sanketplus](https://github.com/sanketplus/PyDFS/commits?author=sanketplus)'s code. The entire file system consists of three parts, namely one **master**, multiple **chunkserver** and one **client**.

+ master

  >Maintain all file system metadata, file-to-block mapping

+ chunkserver

  > Store data and transfer data with the client

+ client

  > Provide api to users for their convenience

### Requirements:

+ rpyc (most important!!! )
+ other basic packages ~~~

### Run

##### For Single computer 

Use different ports for master and chunkserver services. And chunkserver use different paths to represent local disks

1. Edit `gfs.init` for setting *block size*, *replication num*, *master's port* and *chunkserver's port*.
2. Edit `chunkServer.bat` for setting chunkserves. **NOTE:** This must be consistent with the `gfs.init`. And provied file is for windows. For other os, you are expected to modify it.
3. run master.py to start master  with command `python master.py`
4. run mutiple chunkservers  with command `chunkServer.bat` 
5. run client to start a client with command `python client.py`

And some command in **GeekGFS**

```bash
put 1.txt 1.txt # write: put local file 1.txt to GeekGFS, and store as 1.txt
get 1.txt 11.txt # read: get 1.txt in GeekGFS and download it as 11.txt
exist 1.txt  # exist: Determine whether 1.txt exists in GeekGFS
delete 1.txt  # delete: delete 1.txt in GeekGFS
append 1.txt 2.txt  # append: append local file 2.txt to GeekGFS file 1.txt. 
```

### TODO

- [ ] mutiple masters
- [ ] chunkserver heartbeat detection
- [ ] Assign appropriate chunkservers to clients
- [ ] Garbage collection
- [ ] replication allocation
- [ ] .....

