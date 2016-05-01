A way to install latest Spark on Cloudera Manager
=======

Requirements
------------

 * Maven 3 (to build)

## Building and using the Spark CSD

The CSD can be build by running:

```bash
$ mvn package
```

The CSD itself is a jar file located under the target
directory of each CSD. For Spark, the CSD is located:

```bash
$ ls target/SPARK-5.5.1.jar
```

Replace Spark CSD:

```bash
$ scp target/SPARK-5.5.1.jar root@ClouderaManagerServer:/usr/share/cmf/common_jars/
```

Restart Cloudera Manager server:

```bash
$ service cloudera-scm-server restart
```

Download [latest Spark parcel](http://baidu.com) and install it:

```bash
$ scp YSPARK-1.6.1-1.cdh5.4.3.p04.28-el6.parcel root@ClouderaManagerServer:/opt/cloudera/parcel-repo/   
```

```bash
$ scp YSPARK-1.6.1-1.cdh5.4.3.p04.28-el6.parcel.sha root@ClouderaManagerServer:/opt/cloudera/parcel-repo/
```

Change the owner:

```bash
$ chown cloudera-scm:cloudera-scm /opt/cloudera/parcel-repo/YSPARK-1.6.1-1.cdh5.4.3.p04.28-el6.parcel*
```

Then you can install Spark Standalone as normal.

All source in this repository is [Apache-Licensed](LICENSE.txt).

