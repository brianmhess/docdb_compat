# Amazon DocumentDB Compatibility Tool
This compatibility tool will examine log files from MongoDB
to determine if there are any queries which use operators that
are not supported in Amzon DocumentDB. This tool will produce a
simple report of use of unsupported operators, as well as saving
all log lines that were not supported being saved to an output
file for further investigation.

## Installation
Clone the repository, then run the following command in the repository
top-level directory: 
```
pip3 install -r requirements.txt
```

## Using the tool
This tool supports examining compatibility with either the 3.6
or 4.0 versions of Amazon DocumentDB. The format of the command is:
```
python3 docdb_compat/compat.py <version> <input log file> <output file>
```

* The `<version>` is the version of Amazon DocumentDB with which you
are evaluating compatibility.
* The `<input log file>` is the MongoDB log file to process
* The `<output file>` is where all log lines that contain operators
which are not supported by Amazon DocumentDB will saved

### Examples
```
python3 docdb_compat/compat.py 3.6 test/testlog.txt /tmp/test.output
```
Expected output:
```
Results:
        2 out of 7 queries unsupported
Query Types:
        aggregate   3
        find        3
        query       1
Unsuported operators (and number of queries used)
        $facet                2
        $bucket               1
        $bucketAuto           1
Log lines of unsupported operators logged here: /tmp/compat.out
```
