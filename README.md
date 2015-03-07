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

Since 1.8.0 Couchdoop has idiomatic intergration with Spark. However, this
version only supports exporting from Hadoop storage to Couchbase. Scala
sources were compiled for Scala 2.10.

The Command Line Tool
---------------------

Couchdoop command line tool provides a way to:

* **import** data from Couchbase into Hadoop by using a Couchbase view
* **export** data from a Hadoop CSV file into Couchbase.

The tool is implementated as a Hadoop `Tool` job so it can be run by providing
the Couchdoop JAR to `hadoop jar` command.  In order to create the JAR you need
to compile the project with Maven. Run the following command in Couchdoop root
directory:

```
mvn package
```

The file `target/couchdoop-${VERSION}-job.jar` will be created (replace
`${VERSION}` with the actual version).  If you run the tool without arguments
you get a usage with the tools available:

```
hadoop jar target/couchdoop-${VERSION}-job.jar
```

Pass only the tool name as the first argument and the usage for that tool is
displayed.  Use `import` and `serial-import` for importing and `export` for
exporting.

### Importing ###

Couchdoop is able to import data from Couchbase by using a view into HDFS
files. The following line prints the usage for the tool:

```
hadoop jar target/couchdoop-${VERSION}-job.jar import
```

Add options as explained in the below table to perform an import:

| Option                              | Description
| ----------------------------------- | -----------
| `-h`,`--couchbase-urls`             | (required) comma separated URL list of one or more Couchbase nodes from the cluster
| `-b`,`--couchbase-bucket`           | (required) bucket name in the cluster you wish to use
| `-p`,`--couchbase-password`         | (required) password for the bucket
| `-d`,`--couchbase-designdoc-name`   | (required) name of the design document
| `-v`,`--couchbase-view-name`        | (required) name of the view
| `-k`,`--couchbase-view-keys`        | (required) semicolon separated list of view keys (in JSON format) which are going to be distributed to mappers
| `-o`,`--output`                     | (required) HDFS output directory
| `-P`,`--couchbase-view-docsperpage` | buffer of documents which are going to be retrieved at once at a mapper; defaults to 1024
| `-m`,`--hadoop-mappers`             | number of mappers to be used by Hadoop; by default it will be equal to the number of Couchbase view keys passed to the job

The following example imports all documents from Couchbase view "clicks" from
design document "tracking".  See [Couchbase
Views](http://docs.couchbase.com/couchbase-sdk-java-1.4/#querying-views)
documentation for more details about views. The output is written in HDFS in
file "/user/johnny/clicks".

```bash
hadoop jar target/couchdoop-${VERSION}-job.jar import \
    --couchbase-urls http://couchbase.example.com:8091/pools \
    --couchbase-bucket my_bucket \
    --couchbase-designdoc-name tracking \
    --couchbase-view-name clicks \
    --couchbase-view-keys '["20140401",0];["20140401",1]' \
    --output /user/johnny/output
```

##### Specifying keys and key ranges  #####

For argument `--couchbase-view-keys` you must provide a ';' separated list of
keys used to query the view.  Couchbase accepts array keys for views. In the
above example two keys are passed. 

You can also include numeric ranges within the specified keys by using a
_double parenthesis_ pattern anywhere inside the keys string passed to
`--couchbase-view-keys`, but _only once_ within each key.
 
`'["20140401",((0-19))];["20140402",((0-19))]'` will generate a total of 40
keys equivalent to  `'["20140401",0];["20140401",1];["20140401",2]; .... ["20140401",19];["20140402",0];["20140401",1]; .... ["20140402",19]'`

`'["201404((01-30))",0];["201404((01-30))",1]'` will correctly generate all
dates within the month of april, automatically detecting leading zeros.
 
`'["201404((01-30))",((0-19))]'` is currently __not__ possible. 


You can control the parallelism with the number of keys you provide and with
the `--hadoop-mappers` argument.  If the `--hadoop-mappers` argument is not
used, each Hadoop _map_ task will take one key to query the view; otherwise the
keys will be divided to the specified number of mappers.  Each mapper will have
to query at least a whole key, so you can't have more mappers than Couchbase
keys.

##### Preparing a Couchbase View #####

Let's see how it's possible to _index_ the data within a bucket by date using a
__view__. Furthermore we will want to also randomly partition data (into a
fixed number of partitions) within each date to allow for better parallelism
control or data sampling.

We assume that we want to import user click events from Couchbase. For each
event we create a document that contains the date in the key.  For example, key
`click::johnny::1404736126`, contains username "Johnny" and the UNIX time
1404736126 when the click was performed.  Behind the key we store the following
document which tracks the fact that the user clicked button download on pixel
coordinate (23, 46):

```json
{
  "button": "Download",
  "xCoord": 23,
  "yCoord": 46
}
```

We create a Couchbase design document named "tracking" which contains a view
"clicks" with the following `map` function:

```javascript
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

By emitting an array which has the date as the first element this view allows
us to import the events by date.  The second element of the emitted array is a
partition number which allows Couchdoop to split the view query between Hadoop
map tasks.  If we want to import all click events for April 1, 2014, we use
option `--couchbase-view-keys '["20140401",0];["20140401",1]'`. Alternativelly,
we can use key ranges: `--couchbase-view-keys '["20140401",((0-1))]`.  By
default, one Hadoop map task will query Couchbase for view key `["20140401",0]`
and another one for view key `["20140401",1]`.

The import tool uses `TextOutputFormat` and will emit Couchbase document IDs as
Hadoop keys and Couchbase JSON documents as Hadoop values. By default, Hadoop
uses tabs as separators between keys and values, but this can be changed by
setting `mapred.textoutputformat.separator` property.

### Exporting ###

Couchdoop _export_ tool is able to export a key-value CSV file form HDFS into
Couchbase. The following command displays the usage for this tool:

```bash
hadoop jar target/couchdoop-${VERSION}-job.jar export
```

Add options as shown in the following table to perform the export.

| Option                       | Description
| ---------------------------- | -----------
| `-h`,`--couchbase-urls`      | (required) comma separated URL list of one or more Couchbase nodes from the cluster
| `-b`,`--couchbase-bucket`    | (required) bucket name in the cluster you wish to use
| `-p`,`--couchbase-password`  | (required) password for the bucket
| `-i`,`--input`               | (required) HDFS input directory
| `-t`,`--couchbase-operation` | one of Couchbase store operations: SET, ADD, REPLACE, APPEND, PREPEND, DELETE, EXISTS; defaults to SET
| `-x`,`--couchbase-expiry`    | Couchbase document expiry value; defaults to 0 (doesn't expire)
| `-d`,`--delimiter-fields`    | Fields delimiter for the CSV input; defaults to tab

The following example shows how to export CSV file "documents.csv" from HDFS to
Couchbase bucket "my_bucket".

```bash
hadoop jar couchdoop-1.3.0-SNAPSHOT-job.jar export \
    -h http://avira5:8091/pools \
    -b AV_Lists -p 'secret' \
    -i /user/johnny/documents.csv \
    -t ADD \
    -x 3600 -d ','
```

The documents are set to expire in 1 hour (3600 s). Argument `-t` or
`--couchbase-operation` tells the tool what Couchbase store operation to use.
This example uses ADD which creates a document if it doesn't exist and lets it
unchanged if it already exists.

The CSV input document must contain two columns, the first one being the
document key and the second one the document value.  By default, Couchdoop uses
tabs as CSV column separators (to be more rigorous this is actually a TSV
file).  In the above example we will tell the tool to use commas as separators.

If you want to delete documents from Couchbase, you must pass option `-t
DELETE` and use a CSV file with just one column, where on each row there must
be a document ID.


The library
-----------

Couchdoop can be used as a library for your MapReduce jobs if you want to use
Couchbase as __input__ or __output__ for your data.  To __update__ existing
Couchbase documents from your Hadoop job, you can extend
`CouchbaseUpdateMapper`.

Couchdoop uses [Semantic Versioning](http://semver.org/) so if your project
depends on _couchdoop_ library minor and patch releases will not break
compatibility.

### Arguments and Properties ###

Various components of the Couchdoop library rely on having certain properties
configured within the Hadoop `Configuration` object.  This is why all command
line argument options available for import and export tools have a
corresponding Hadoop Configuration property.  For example,
`--couchbase-view-keys` corresponds to the `couchbase.view.keys` Hadoop
property.  Just remove the two dashes from the beginning of the long option
names and replace the rest of the dashes with dots.

For example, if you are using Couchdoop in your Hadoop job to read data from
Couchbase, you'll have to set the `couchbase.bucket` Hadoop property (along
with the other properties required for the import) in order to have the
`CouchbaseViewInputFormat` work correctly.

While you can take care of setting these properties yourself, you can also
choose to have your Hadoop job use the same options as Couchdoop's import or
export tool to keep things simple.
 
To load all the properties needed for import from the command line arguments
(`String[] args`) into the Hadoop Configuration object (`Configuration conf`)
you can use the `ArgsHelper` class like this:

```java
ArgsHelper.loadCliArgsIntoHadoopConf(conf, ImportViewArgs.ARGS_LIST, args); 
``` 

This way, if you pass `--couchbase-bucket my_bucket` as an argument, it will
update the configuration object with property `couchbase.bucket = my_bucket`.

If you also need to read these properties in your code you can use the _getter_
methods of the `ImportArgs` class by creating a new instance like this:  
 
```java
ImportViewArgs importViewArgs = new ImportViewArgs(conf);
```

You can also extend ImportViewArgs to include more parameters if you need so,
just like
[`ImportViewArgs`](/src/main/java/com/avira/couchdoop/imp/ImportViewArgs.java)
extends [`ExportArgs`](/src/main/java/com/avira/couchdoop/CouchbaseArgs.java).
 
The same idea applies to `ExportArgs`.

### Couchbase as Hadoop InputFormat ###

Couchbase views can be used as Hadoop `InputFormat` by using
[`CouchbaseViewInputFormat`](/src/main/java/com/avira/couchdoop/imp/CouchbaseViewInputFormat.java).
Check
[`CouchbaseViewImporter`](/src/main/java/com/avira/couchdoop/jobs/CouchbaseViewImporter.java)
class as an example of how to configure a job with `CouchbaseViewInputFormat`.

If you don't want to use `ArgsHelper`, `CouchbaseViewInputFormat` contains the
static method helper `initJob` which configures database and view connection
and sets the `InputFormat`.

The following Mapper maps Couchbase key-values to Hadoop key-values.

```java
public class CouchbaseViewToFileMapper extends Mapper<Text, ViewRow, Text, Text> {

  @Override
  protected void map(Text key, ViewRow value, Context context) throws IOException, InterruptedException {
    if (value != null) {
      context.write(key, new Text(value.getDocument().toString()));
    }
  }
}
```

The Mapper input keys are Couchbase document IDs and the input values are
Couchbase view query results, `ViewRow` objects.

### Couchbase as Hadoop OutputFormat ###

You can write documents to Couchbase by using
[`CouchbaseOutputFormat`](/src/main/java/com/avira/couchdoop/exp/CouchbaseOutputFormat.java).
Check
[`CouchbaseExporter`](/src/main/java/com/avira/couchdoop/jobs/CouchbaseExporter.java)
class as an example of how to configure a job with `CouchbaseOutputFormat`.

`CouchbaseOutputFormat` contains the static method helper `initJob` which
configures database connection, sets the `InputFormat` and set the job output
key and output value types required by `CouchbaseOutputFormat`.

In order to write data to Couchbase you must write a Mapper which outputs
Couchbase document IDs as keys and instances of `CouchbaseAction` as values. A
`CouchbaseAction` instance associates a Couchbase document value with a store
operation, so for each value you want to store you can tell Couchdoop if you
want to use `set`, `add`, `replace`, `append`, `prepend` or `delete`. 

### Updating documents from Couchbase with Hadoop ###

Couchbase existing documents can be updated by using other data from Hadoop
storage. In order to do this you must configure the job to use
`CouchbaseOutputFormat` and extend class
[`CouchbaseUpdateMapper`](/src/main/java/com/avira/couchdoop/update/CouchbaseUpdateMapper.java).
You can use whatever `InputFormat` you want. The following abstract methods
need to be implemented:

* `transform` method receives the key and the value from the Hadoop storage and
  extracts from it a Couchbase key, which will be used to get an existing
  document from Couchbase, and a generic value, `hadoopData`, which will be
  merged with the existing Couchbase document in order to create a new
  document.
* `merge` method computes a new Couchbase document by using a value extracted
  from Hadoop storage and an existing Couchbase document.

Running Couchdoop on Spark
--------------------------

The integration of Couchdoop with Apache Spark uses `CouchbaseOutputFormat` and
some syntactic sugar for idiomatic Spark/Scala API. Currently, only exporting
data to Couchbase is supported. However, you can manually use
`CouchbaseViewInputFormat` and `saveAsNewAPIHadoopDataset` RDD method from
Spark. Check Spark documentation for using Hadoop storage. Also, the
integration is only available for Scala programmers.

### Exporting to Couchbase with Spark

In order to export data from Spark to Couchbase, first import `CouchdoopSpark`
functions into the scope:

```scala
import com.avira.couchdoop.spark.CouchdoopSpark._
```

Create an implicit object with Couchbase connection settings:

```scala
implicit val cbOutputConf = CouchdoopExportConf(
  urls = Seq("http://example.com:8091/pools"),
  bucket = "my_bucket",
  password = "secret"
)
```

Then reformat your RDDs to a pair of Couchbase key (as String) and a
`CouchbaseAction` object, which encapsulates the Couchbase operation and the value
(document):

```scala
val cbOutput = sc.parallelize(
  Seq(
    ("key::1", CouchbaseAction.createSetAction("value_1")),
    ("key::2", CouchbaseAction.createAddAction("value_2")),
    ("key::3", CouchbaseAction.createDeleteAction)
  )
)
```

And finally, save the RDD to Couchbase:

```scala
cbOutput.saveToCouchbase
```

For an example project check out [couchdoop-spark-demo GitHub
project](https://github.com/Avira/couchdoop-spark-demo).

Dependency issues
-----------------

At Avira we experienced a dependency issues when running Couchdoop on CDH5.
CDH5's Hadoop classpath includes dependency org.apache.httpcomponents:httpcore
version 4.2.5, while the Couchbase Java Client includes a newer version of this
library, 4.3. Before Couchdoop 1.8, in order to make it work we needed to
prioritize 4.3 by either setting `mapreduce.task.classpath.user.precedence` job
property to `true` or by calling `setUserClassesTakesPrecedence(true)` on the
`Job` object. Since Couchdoop 1.8 this is not required any more because we
shaded httpcore classes. Although shading might create other problems we chose
to do this in order to make Spark work with Couchdoop. If you think you have a
better solution feel free to contribute to the problem by issuing a pull
request with a patch.
