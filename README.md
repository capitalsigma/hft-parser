# Quickstart

The easiest way to run the parser is with the `run.sh` shell script. The syntax for using it is:

    ./run.sh [infile].csv.gz [outfile].h5 [config].json [num_per_pass]

Here, `[infile].cs.gz` is the input ARCABook file, `[outfile].h5` is the name of the h5 file where the output should be saved, `[config].json` is the configuration file to use, and `[num_per_pass]` is the number of symbols to include in each pass. These options will be described in more detail later. Currently, `run.sh` uses the default symbol set. If another symbol set is preferred, it is best to edit this shell script and set it here for every run.

This will save a copy of the  program's output in the default log directory, `logs/`, in addition to the contents of `config.json` and the symbols used. The logfile contains the total runtime, the Java heapsize used, and the number of times that the "line queue" and "datapoint" queues were unsuccessfully enqueued to/dequeued from. The purpose of including this information is so that after test runs, it is possible to know exactly what configuration was responsible for the runtime. Note that the program itself doesn't do any logging -- it relies on you to capture its output to stdout somewhere.

The most important part of this script is the option `MAX_MEM` at the top. It sets the Java heap size. It should be edited once on each machine the program is copied to to account for the amount of memory available on that machine.

# Compiling

Use the `pom.xml` included to compile with Maven, via `mvn compile`.

# Running from the commandline

Run the `jar` produced by Maven as follows:

    java -jar PATH_TO_JAR.jar -XmsN -XmsN -d64 [OPTIONS]

Where `N` is the Java heap size. The Java heap size is the maximum amount of memory that the JVM is allowed to use once the program starts. It is *extremely* memory hungry, and this *must* be raised above the default for even a moderately large run.

The following commandline options are accepted:

* `-symbols/-s`: A text file containing the list of symbols, one per line
* `-book /-b`: Gzipped CSV of book data
* `-out/-o`: Output .h5 file
* `-config/-c`: JSON-formatted config file
* `-num/-n`: Number of symbols per run

The configuration file format will be addressed later. The `-n` option is unintuitive: it controls the number of symbols from the symbol list that will be used on each pass through the file. This option controls them memory usage: the more symbols per pass, the higher the memory usage.

# Configuration file format
Here is an example configuration file:

```
{
    "ParseRun" : {
        "line_queue_size": 500000,
        "point_queue_size": 500000,
        "backoff": true,
		"min_backoff_s": 20,
		"max_backoff_s": 500,
		"min_backoff_d": 20,
		"max_backoff_d": 500
	},
	"ArcaParser": {
		"initial_order_history_size": 500000,
		"output_progress_every": 5000000
	},
	"HDF5Writer": {
		"start_size": 50000,
		"chunk_size":  100000,
		"sync_mode": "SYNC",
		"overwrite": true,
		"keep_datasets_if_they_exist": true,
		"perform_numeric_conversions": false
	},
	"HDF5CompoundDSBridge" : {
		"default_storage_layout": false,
		"storage_layout": "CHUNKED",
		"deflate_level": 1,
		"cache_size": 100000,
		"cutoff": true,
		"async": true,
		"core_pool_size": 9,
		"max_pool_size": 9,
		"keep_alive_sec": 30,
		"queue_size": 5000,
		"parallel_write": true
	},
    "MarketOrderCollection": {
        "start_capacity": 5000,
        "max_key_count": 10,
        "caching": true
    }
}
```

Each inner section controls a different part of the parser. This file is `config-files/caching-100000-async-9-threads-parflush.json`, which, at the moment, is the most efficient configuration file.

## `ParseRun` options

These probably don't need to be changed. The two queue sizes determine the size of the "line queue" (the queue of lines from the book file waiting to be parsed) and the "point queue" (the queue of datapoints from the parser waiting to be written). The other options control the amount of time to wait after an unsuccessful enqueue/dequeue operation.

## `ArcaParser` options

Thse probably don't need to be changed.

* `"initial_order_history_size"`: determines the original size of the list of past orders.
* `"output_progress_every"`: determines how often, in lines, the parser reports its progress.

## `HDF5Writer` options

These options control the part of the file that writes out datapoints to the HDF5 file.

* `"start_size"`: determines the initial number of top-level entries (symbols) in the output file
* `"chunk_size"`: determines the number of datapoints written out in a single operation. It should match `chunk_size` in the `HDF5CompoundDSBridge` settings for maximum efficiency (both output .h5 size and runtime).
* `"overwrite"`: determines whether to overwrite an existing HDF5 file of the same name as the output file specified or error.

The other two options are low-level options to the underlying HDF5 writer code that seem not to make a difference.

## `HDF5CompoundDSBridge` options

* `"default_storage_layout"`: a low-level option to the writer.
* `"storage_layout"`: needs to be `CHUNKED`.
* `"deflate_level"`: the compression level, from 1-9. Higher means more compressed and slower.
* `"cache_size"`: the number of datapoints to collect before writing out.
* `"cutoff"`: if the last write operation is not equal to a full cache, then either write out zeroed-out datapoints (`cutoff=false`) to fill it out, or clip off the empty points (`cutoff=true`)
* `"async"`: perform writes synchronously or asynchronously. If `false`, the remaining settings have no effect. Should be `true` for best efficiency.
* `"core_pool_size"`: the minimum number of threads in in the writer threadpool, shared across every writer.
* `"max_pool_size"`: the maximum number of threads. Should be equal to `core_pool_size` for best effiency.
* `"keep_alive_sec"`: the number of seconds to keep alive a thread with nothing to do.
* `"queue_size"`: the size of the `ArrayBlockingQueue` supplying work to the threadpool.
* `"parallel_write"`: whether or or not to perform the final flush of the cache asynchronously (`true`) or synchronously (`false`). Small, positive effect on efficiency.

## `MarketOrderCollection` options

* `"start_capacity"`: the starting size of the hashmap of prices and quantities for each symbol.
* `"max_key_count"`: the number of unique prices to write out for each type of order (e.g. `max_key_count = 10` writes out the 10 highest buy orders and 10 lowest sell orders)
* `"caching"`: if `false`, then the top N orders are recomputed every tick. If `true`, then the top N orders are saved each tick and recomputed only if the order collection has changed since the past tick. Should be `true` for best efficiency.

<!-- TODO: help option on binary. check mem requirements  -->
