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

Using [latest Spark parcel](http://ec2-52-25-166-54.us-west-2.compute.amazonaws.com/spark/):
```
http://ec2-52-25-166-54.us-west-2.compute.amazonaws.com/spark/
```

All source in this repository is [Apache-Licensed](LICENSE.txt).

