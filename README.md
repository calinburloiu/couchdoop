Couchdoop
=========

Couchdoop is a Hadoop connector for Couchbase which is able import, export and
update data. The connector can be used both as a command line tool, which works
with CSV files from HDFS, and as a library for MapReduce jobs, for more
flexibility.

The library provides a Hadoop `InputFormat` which is able to read data from
Couchbase by querying a view and an `OutputFormat` which can store in Couchbase
key-value pairs read from any Hadoop source. The `OutputFormat` also allows
other useful operations like deleting, counting or changing the expiry of some
documents.

Couchdoop can be used to update some existing Couchbase documents by using data
from other Hadoop sources. Imagine a recommendation system which stores item
scores in Couchbase documents. After rerunning a machine learning algorithm
over user events data from Hadoop the scores from Couchbase can be updated
directly.

Couchdoop aims to be a better alternative for the official [Couchbase Sqoop
connector](http://www.couchbase.com/couchbase-server/connectors/hadoop) which
is only able to import a full bucket or to stream documents for a configurable
amount of time.

The Command Line Tool
---------------------

Couchdoop command line tool provides a way to:

* **import** data from Couchbase into Hadoop by using a Couchbase view
* **export** data from a Hadoop CSV file into Couchbase.

The tool is implementated as a Hadoop `Tool` job so it can be run by providing the Couchdoop JAR to `hadoop jar` command. In order to create the JAR you need to compile the project with Maven. Run the following command in Couchdoop root directory:

```
mvn package
```

The file `target/couchdoop-${VERSION}-job.jar` will be created (replace `${VERSION}` with the actual version). If you run the tool without arguments you get a usage with the tools available:

```
hadoop jar target/couchdoop-${VERSION}-job.jar
```

Pass only the tool name as the first argument and the usage for that tool is displayed. Use `import` and `serial-import` for importing and `export` for exporting.

### Importing ###

Couchdoop is able to import data from Couchbase by using a view into HDFS files. The following line prints the usage for the tool:

```
hadoop jar target/couchdoop-${VERSION}-job.jar import
```

Add options as explained in the below table to perform an import:

| Option                           | Description
| -------------------------------- | -----------
| -h,--couchbase-urls              | (required) comma separated URL list of one or more Couchbase nodes from the cluster
| -b,--couchbase-bucket            | (required) bucket name in the cluster you wish to use
| -p,--couchbase-password          | (required) password for the bucket
| -d,--couchbase-designdoc-name    | (required) name of the design document
| -v,--couchbase-view-name         | (required) name of the view
| -k,--couchbase-view-keys         | (required) semicolon separated list of view keys (in JSON format) which are going to be distributed to mappers
| -o,--output                      | (required) HDFS output directory
| -P,--couchbase-view-docsperpage  | buffer of documents which are going to be retrieved at once at a mapper; defaults to 1024

The following example imports all documents from Couchbase view "clicks" from design document "tracking". See [Couchbase Views](http://docs.couchbase.com/couchbase-sdk-java-1.4/#querying-views) documentation for more details about views. The output is written in HDFS in file "/user/johnny/clicks".

```
hadoop jar target/couchdoop-${VERSION}-job.jar import \
    --couchbase-urls http://couchbase.example.com:8091/pools \
    --couchbase-bucket my_bucket \
    --couchbase-designdoc-name tracking \
    --couchbase-view-name clicks \
    --couchbase-view-keys '["20140401",0];["20140401",1]' \
    --output /user/johnny/output
```

The argument `--couchbase-view-keys` you must provide a ';' separated list of keys used to query the view. Couchbase accepts array keys for views. In the above example two keys are passed. Each Hadoop _map_ task will take a key to query the view. The many the keys you provide here the more you increase the parallelism.

Let's assume that we want to import user click events from Couchbase. For each event we create a document that contains the date in the key. For example, key "click::johnny::1404736126", contains username "Johnny" and the UNIX time 1404736126 when the click was performed. Behind the key we store the following document which tracks the fact that the user clicked button download on pixel coordinate (23, 46):

```
{
  "button": "Download",
  "xCoord": 23,
  "yCoord": 46
}
```

We create Couchbase design document "tracking" which contains a view "clicks" with the following `map` function:

```
function (doc, meta) {
  // Retrieve only JSON documents.
  if (meta.type != "json") {
    return;
  }

  var NUM_PARTITIONS = 2;
  var re = /click::([\w]+)::([\d]+)/
  var fields = re.exec(meta.id);
  var user = fields[1];
  var ts = fields[2];

  // Compute the date in a format like "20140401".
  var dateObj = new Date(ts * 1000);
  var date = '' + dateObj.getFullYear() +
      ('0' + (dateObj.getMonth() + 1)).slice(-2) +
      ('0' + dateObj.getDate()).slice(-2);

  // Compute sample number based on seconds (should provide enough randomness).
  var partition = dateObj.getSeconds() % NUM_PARTITIONS;
 
  emit([date, partition], null);
}
```

By emitting an array which has the date as the first element this view allows us to import the events by date. The second element of the emitted array is a partition number which allows Couchdoop to split the view query between Hadoop map tasks. If we want to import all click events for April 1, 2014, we use option `--couchbase-view-keys '["20140401",0];["20140401",1]'`. A Hadoop map task will query Couchbase for view key `["20140401",0]` and another one for view key `["20140401",1]`.

The import tool uses `TextOutputFormat` and will emit Couchbase document IDs are Hadoop keys and Couchbase JSON documents as Hadoop values. By default, Hadoop uses tabs as separators between keys and values, but this can be changed by setting `mapred.textoutputformat.separator` property.

### Exporting ###

Couchdoop _export_ tool is able to export a key-value CSV file form HDFS into Couchbase. The following command displays the usage for this tool:

```
hadoop jar target/couchdoop-${VERSION}-job.jar export
```

Add options as shown in the following table to perform the export.

| Option                   | Description
| ------------------------ | -----------
| -h,--couchbase-urls      | (required) comma separated URL list of one or more Couchbase nodes from the cluster
| -b,--couchbase-bucket    | (required) bucket name in the cluster you wish to use
| -p,--couchbase-password  | (required) password for the bucket
| -i,--input               | (required) HDFS input directory
| -t,--couchbase-operation | one of Couchbase store operations: SET, ADD, REPLACE, APPEND, PREPEND, DELETE, EXISTS; defaults to SET
| -x,--couchbase-expiry    | Couchbase document expiry value; defaults to 0 (doesn't expire)
| -d,--delimiter-fields    | Fields delimiter for the CSV input; defaults to tab

The following example shows how to export CSV file "documents.csv" from HDFS to Couchbase bucket "my_bucket".

```
hadoop jar couchdoop-1.3.0-SNAPSHOT-job.jar export \
    -h http://avira5:8091/pools \
    -b AV_Lists -p 'secret' \
    -i /user/johnny/documents.csv \
    -t ADD \
    -x 3600 -d ','
```

The documents are set to expire in 1 hour (3600 s). Argument `-t` or `--couchbase-operation` tells the tool what Couchbase store operation to use. This example uses ADD which creates a document if it doesn't exist and lets it unchanged if it already exists.

The CSV input document must contain two columns, the first one being the document key and the second one the document value. By default, Couchdoop uses tabs as CSV column separators. (To be more rigorous this is a TSV file.) In the above example will tell the tool to use commas as separators.

Running on CDH5
---------------

At Avira we experienced a dependency issue when running Couchdoop on CDH5 (in
particular CDH 5.0.2). CDH5's Hadoop classpath includes dependency
org.apache.httpcomponents:httpcore version 4.2.5, while the Couchbase Java
Client includes a newer version of this library, 4.3. In order to make it work
we needed to prioritize 4.3 by either setting
`mapreduce.task.classpath.user.precedence` job property to `true` or by calling
`setUserClassesTakesPrecedence(true)` on a the `Job` object.
