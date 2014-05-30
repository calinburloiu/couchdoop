Couchbase-Hadoop Connector
==========================

Couchdoop is a Hadoop connector for Couchbase which is able import, export and
update data. The connector can be used both as a command line tool, which works
with CSV files from HDFS, and as a library for MapReduce jobs, for more
flexibility.

The library provides a Hadoop InputFormat which is able to read data from
Couchbase by querying a view and an OutputFormat which can store in Couchbase
key-value pairs read from any Hadoop source. The OutputFormat also allows other
useful operations like deleting, counting or changing the expiry of some
documents.

Couchdoop can be used to update some existing Couchbase documents by using data
from other Hadoop sources. Imagine a recommendation system which stores item 
scores in Couchbase documents. After rerunning a machine learning algorithm over 
user events data from Hadoop the scores from Couchbase can be updated directly.

Couchdoop aims to be a better alternative for the official Couchbase Sqoop
connector which is only able to import a full bucket or to stream documents for
a configurable amount of time.
