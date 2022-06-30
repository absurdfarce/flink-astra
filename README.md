# What Is This?
A simple example of how to use [DataStax Astra](http://astra.datastax.com/) as a Cassandra sink within [Apache Flink](https://flink.apache.org/)

# Overview
[Recent work](https://github.com/apache/flink/commit/15f18dba2b2cd82c14c5dbdb617641119fd5bfcd) on Apache Flink upgraded the version of the [DataStax Java driver for Apache Cassandra](https://github.com/datastax/java-driver) used by the project for interacting with Apache Cassandra databases.  The new version of the Java driver supports interaction with DataStax Astra, opening up the possibility of using Astra as a sink for results computed by Flink.  This repository is intended to demonstrate how to enable such support when using a Flink DataStream.

This code is intended as a fairly simple demonstration of how to enable an Apache Flink job to interact with DataStax Astra.  There is certainly room for optimization here.  A simple example: Flink's CassandraSink will open a new Session on each open() call even though these Session objects are thread-safe.  A more robust implementation would be more aggressive about memoizing Sessions, encouraging a minimal number of open sessions for multiple operations on the same JVM.  This work may be undertaken in the future, but for the moment it is beyond the scope of what we're aiming for here.

# How Do You Run It?
The code requires a running database on DataStax Astra.  Once you have such a database the following additional steps are necessary:

* Create a keyspace named "example" in your Astra database.  At the moment this name is hard-coded.
* Download the secure connect bundle (SCB) for your database and place it in app/src/main/resources [1]
* Create a properties file at app/src/main/resources/app.properties.
* Add properties specifying your Astra username, password and SCB file name.  These should map to the "astra.clientid", "astra.secret" and "astra.scb" properties respectively.

Note that most of the configuration specified above is driven by the sample app; it's not required by the general method described here.  You're free to implement alternate methods for loading Astra username/password information and/or your SCB.

With this configuration in place you should now be ready to run the application.  This should be as simple as:

> ./gradlew run

Verify that the application runs and exits normally (Gradle will report this as a "successful build").  Once you've verified a clean run you can check to make sure you have data in your DataStax Astra database.  If you're using the data defined in the sample app you should see something very much like the following:

```
token@cqlshselect * from example.wordcount ;

 word   | count
--------+-------
   dogs |     1
 lazier |     1
  least |     1
  foxes |     1
 jumped |     1
     at |     1
    are |     1
   just |     1
  quick |     1
   than |     1
    fox |     1
    our |     1
    dog |     2
     or |     1
   over |     1
  brown |     1
   lazy |     1
    the |     2

(18 rows)
```

[1] You can actually place this file anywhere on the classpath since we use ClassLoader's getResourceAsStream() method to find it.  app/src/main/resources is simply the most convenient place to do so.

