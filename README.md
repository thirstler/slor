SLoR
====

S3 Load Ruler is a (relatively) easy-to-use benchmarking and load generation tool
for S3 storage. 

What does it do?
----------------

Slor can be used to generate load and measure the performance capabilities of
S3 storage systems. It will measure throughput, bandwidth and processing times
of several basic S3 operations (read, write, head, delete, overwrite, reread,
MPUs and read-ranges) in discrete and mixed workload configurations. 

How does it do?
---------------
It operates as a system of load-generators (drivers) that are instructed to do 
"things" by a controller process. This is similar to other distributed load
generation systems:

    Controller ___ Driver1
                \_ Driver2
                \_ Driver3
                \_ etc...

You start drivers on the systems you want to use for load generation. Then
you will reference these servers when starting a workload with a slor
"controller" process. The controller is responsible for sending workloads to
driver processes and then collecting the returned performance data.

Where does it do?
-----------------

It should run anywhere Python3 runs (though it is more tested on POSIX type
systems). The limiting factor is the use of the "pickle" module. Data moved
between controller and drivers is serialized with routines that are not
guaranteed to be compatible between Python versions: no controller with one
major Python version and drivers with another.

Installation
------------

Slor can be installed by building the pip package and installing it.

    cd slor/
    python -mbuild
    pip install --user --upgrade ./dist/slor-[version].tgz

Python virtual environments are good practice in a lot of cases.


Running a Workload
------------------

You can easily start a workload using the "controller" command. Minimally you
need an accessKey/secretKey and an endpoint to use. For example:

    ./slor controller --access-key user --secret-key password --endpoint http://123.45.67.89

A driver process will automatically launch and a default benchmark workload
will be deployed with the following configuration:

* Workload list:
    * read
    * write
    * head
    * mixed (with default profile):
        * 60% read
        * 25% write
        * 5% delete
        * 10% head
    * delete
    * cleanup
* 10 worker threads will launch
* Data will be auto-prepared to accomodate 1000 ops/s for the stage run time (5 min)
* 1MB objects will be used in the workloads
* Each stage will run for 5 minutes
* A single (1) bucket will be used in the test
* Bucket prefix will be "slor-" (resulting bucket name "slor-0")

This is a perfectly good way to start, but you'll likely want to benchmark 
different file sizes, I/O thread counts, workload mixtures, bucket counts,
whatever. You can specify the stages in the workload with the --loads flag.

    ./slor controller --access-key user \
    --secret-key password \
    --endpoint http://123.45.67.89 \
    --loads read,write \
    --cleanup

This will run a read, then write workload with a clean-up stage at the end.
Clean-up will remove ALL objects from the bucket - ALL of them, don't use
existing buckets for benchmarking!

Let's add a mixed workload (that isn't the default). Mixed workloads accept
the following operation classes along with "share" values that say how
often that operation will appear in the stage:

* read
* write
* head
* reread
* overwrite
* delete

Example:

    ./slor controller --access-key user \
    --secret-key password \
    --endpoint http://123.45.67.89 \
    --loads read,write,mixed,cleanup \
    ----mixed-profiles '[{"read": 10, "reread": 2, "write": 4, "overwrite": 1, "delete": 5, "head": 5}]'

You can add multiple stages to a workload of the same type.

    ./slor controller --access-key user \
    --secret-key password \
    --endpoint http://123.45.67.89 \
    --loads read,write,read,cleanup
    
If you want to do this with mixed workloads, add them to the --mixed-profiles
array:

    ./slor controller --access-key user \
    --secret-key password \
    --endpoint http://123.45.67.89 \
    --loads read,mixed,mixed,cleanup \
    --mixed-profiles '[
        {"read": 5, "reread": 1, "write": 2, "overwrite": 1, "delete": 1, "head": 2},
        {"read": 2, "reread": 1, "write": 5, "overwrite": 1, "delete": 1, "head": 2}
    ]'

If you're benchmarking spinning disks you might need to overrun page cache
so that you're testing cold reads from disk rather than data from memory.
This is done with a "blowout" stage. In order to use this effectively you 
need to know how much page cache you have to overrun before you can force the 
system to read from the media. Fail-safe amount is the total amount
of memory in the storage cluster you're testing. If you're targeting a 
cluster with 6 nodes of 512GB a piece you can used 512*6 as your overrun
target (3072GB):

    ./slor controller --access-key user \
    --secret-key password \
    --endpoint http://127.0.0.1 \
    --cachemem-size 3.072T \
    --loads write,blowout,read,cleanup

This will write 3.072TB of data to the system (with 8MB objects) just before
the read stage to "blow out" the page cache and force reads from disk.

Chances are excellent that there are other parameters you'll want to change.
Let change the object size.

    ./slor controller --access-key user \
    --secret-key password \
    --endpoint http://127.0.0.1 \
    --cachemem-size 3.072T \
    --loads write,blowout,read,cleanup \
    --object-size 256K

You can also specify a range of sizes:

    ./slor controller --access-key user \
    --secret-key password \
    --endpoint http://127.0.0.1 \
    --cachemem-size 3.072T \
    --loads write,blowout,read,cleanup \
    --object-size 256K-5M

This selects a random size for each object inside the specified range
(inclusive).

A limitation you may notice is that you can specify several workload stages
but you cannot change the paramaters for each individually. For instance
you can't, in the same workload command, specify a different object size
for the write stage than for the read stage - or separate cachemem-sizes
for blowout stages - or different run-times for each stage. For that you
simply need to run with separate loads with appropriate settings.

Distributed Jobs
----------------

So far we've been using the built-in load-generator that launches with 
slor when you don't specify drivers to attach to. On separate load generation
hosts start the driver with the "driver" sub-command:

    ./slor driver

This launches a driver process (does not daemonize) that attaches to the
default port (9256). Do this on one or more hosts and then launch work
loads from a controller specifying the host names of the hosts running the
drivers:

    ./slor controller --access-key user \
    --secret-key password \
    --endpoint http://127.0.0.1 \
    --cachemem-size 3.072T \
    --loads write,blowout,read,cleanup \
    --object-size 256K-5M \
    --driver-list loadgen1,loadgen2,loadgen3

Now the load will launch and distribute over each host equally. The default
process count is 10 per-driver, so this will launch a total of 30 threads
of IO total.

If you're trying to generate serious load you'll want to increase the per-
driver process count. Use "--processes-per-driver"

    ./slor controller --access-key user \
    --secret-key password \
    --endpoint http://127.0.0.1 \
    --cachemem-size 3.072T \
    --loads write,blowout,read,cleanup \
    --object-size 256K-5M \
    --driver-list loadgen1,loadgen2,loadgen3 \
    --processes-per-driver 45

Now we have 135 simultanious IO streams hitting the S3 endpoint.

Preparing Data
--------------

When doing read workloads you need data in the system to read back. If you're 
trying to realistically assess platform performance then you don't want to re-
read the same data over and over again. This means you need to have _enough_
data to sustain the IO rate for the length of time the benchmark takes to run. 

If this sounds like you need to already know how the targeted platform can be
expected to perform, then you're right. You may not really know, but chances
are you can make some pretty good guesses.
* What are the network limitations?
* What are the IO limitations of the metadata
layer?
* What do you already know from previous workloads or use?

I know this system couldn't possibly take more than 10,000 IO/s of reads at
256KB so let's assume a ceiling of 12,000 IO/s and call it a day. That would
be 3.07 GB/s of bandwidth (256KB * 12,000)

You can tell SLoR the amount of data to auto-prepare with the "--iop-limit"
flag. It will use this in conjunction with the "--stage-time" flag which 
specifies how long each stage will run (defaults to 300 seconds). Let's start
a workload that can sustain 12,000 operations per second of reads for 15
minutes using a 256KB object size:

    ./slor controller --access-key user \
    --secret-key password \
    --endpoint http://127.0.0.1 \
    --cachemem-size 3.072T \
    --loads write,blowout,read \
    --object-size 256K \
    --driver-list loadgen1,loadgen2,loadgen3,loadgen4 \
    --processes-per-driver 64
    --stage-time 900 \
    --iop-limit 12000 \
    --cleanup

This will prepare 2.7TB (10.8 million objects) of 256K files anticipating 
that reads will not exceed 12000 operations a second. It will blow-out
the page cache before the read stage, run each timed stage for 15 min and
clean up when it's done (the "blowout" stage takes as long as it takes).


Other Options
-------------

An other thing you can specify is key length. This can be done one of two
ways (and both at the same time): by using a prefix that's appended to each
key and by specifying the key length itself. This can be useful if you think
metadata length is affecting performance. Let's configure a run using a
key-length of ~120 random characters along with a static prefix appended
to each key. For this I'll use a range of possible key lengths, though I 
could specify a fixed length as well:


    ./slor controller --access-key user \
    --secret-key password \
    --endpoint http://127.0.0.1 \
    --cachemem-size 3.072T \
    --loads write,blowout,read,cleanup \
    --object-size 256K \
    --driver-list loadgen1,loadgen2,loadgen3,loadgen4 \
    --processes-per-driver 64
    --stage-time 900 \
    --iop-limit 12000 \
    --key-length 100-140
    --key-prefix "00000-00-00000/fffffffff/000-0/"


You can add "sleep" stages to the workload if you want to take a nap between
stages. The timing of the sleeps is set (in seconds) with "--sleep-time" flag:

    ./slor controller --access-key user \
    --secret-key password \
    --endpoint http://127.0.0.1 \
    --cachemem-size 3.072T \
    --object-size 256K \
    --driver-list loadgen1,loadgen2,loadgen3,loadgen4 \
    --processes-per-driver 64
    --stage-time 900 \
    --iop-limit 12000 \
    --key-length 100-140
    --key-prefix "00000-00-00000/fffffffff/000-0/" \
    --loads sleep,write,blowout,sleep,read,cleanup \
    --sleep-time 60

Saving Data
-----------

It's painful to see good data go to waste. If you're doing a lot of read-testing
and _not_ deleting data as part for the workloads, you can reuse data with 
the "--save-readmap" flag. It can also be useful if you're breaking down 
workloads across controller commands.

SLoR pre-generates all of the keys it's going to use for prepared data (this is
what's going on during the "readmap" stage). It's essentially a list of of 
bucket/key pairs that's juggled between stages to keep the same drivers from
handing the same objects over and over. If you don't clean-up or delete data
during a run you can save the readmap of a workload with "--save-readmap" and
then use "--use-readmap" in subsequent runs to avoid unnecessary data
generation:

    ./slor controller --access-key user \
    --secret-key password \
    --endpoint http://127.0.0.1 \
    --cachemem-size 3.072T \
    --object-size 256K \
    --driver-list loadgen1,loadgen2,loadgen3,loadgen4 \
    --processes-per-driver 64
    --stage-time 900 \
    --iop-limit 12000 \
    --key-length 100-140
    --key-prefix "00000-00-00000/fffffffff/000-0/" \
    --loads sleep,write,blowout,sleep,read \
    --sleep-time 60 \
    --save-readmap /tmp/readmap.json

Then you can use that data again by loading the readmap later. Keep in mind
that any object size or key information you specify in this command line is
ignored as it pertains to reading data (since it's not prepared by this
command).

    ./slor controller --access-key user \
    --secret-key password \
    --endpoint http://127.0.0.1 \
    --cachemem-size 3.072T \
    --driver-list loadgen1,loadgen2,loadgen3,loadgen4 \
    --processes-per-driver 64
    --stage-time 900 \
    --iop-limit 12000 \
    --loads blowout,read,mixed,delete \
    --mixed-profiles '[{"read": 50, "write": 25, "head": 25}]
    --sleep-time 60 \
    --use-readmap /tmp/readmap.json

Of course, in the above workload, there's a delete stage which makes the saved
readmap file useless after this run. I hope you're happy.

Internals
=========

Drivers and Communication
-------------------------

The controller process (./slor controller) can run anywhere. On a separate
host or on a host running a driver. The controller attaches to the drivers
using the Python multiprocessing module connection functions. The controller
takes the global view of the workload, divides the work equally among the 
driver processes and sends the workloads to the drivers. The drivers, in turn, 
will divide that share of the workload equally among the defined number of
processes per driver. They then spawn those processes and go to town.


    controller _____
     process        \_ Driver on host1
                    |     \_ worker process
                    |     \_ worker process
                    |     \_ worker process
                    |     \_ ...
                    \_ Driver on host2
                    |     \_ worker process
                    |     \_ worker process
                    |     \_ worker process
                    |     \_ ...
                    \_ Driver on host3
                    |     \_ worker process
                    |     \_ ...
                   ...

Workload and control commands are sent from the control process to the 
drivers and then from the drivers the workload processes and back:

Communication Paths
-------------------

    |-------------|---------------------------|
    | Control host|        Driver Node(s)     |
    |-------------|---------------------------|
    | Controller  |   Driver    |   Workers   |
    |-------------|---------------------------|
    |    start    |             |             |
    |   workload  |             |             |
    |      |      |             |             |
    |  initiate   |             |             |
    |  handshake -->   relay    |             |
    |             |  handshake -->  receive   |
    |             |             |      &      |
    |             |   relay &  <--  respond   |      
    |   success  <--  respond   |             |
    |      &      |             |             |
    |   launch    |             |             |
    |  workload   |             |             |
    |      &      |             |             |
    |divide config|             |             |
    |  and send  -->  Receive   |             |
    | to drivers  |  & further  |             |
    |             |divide config|             |   
    |             |  & send to -->  Execute   |
    |             |   workers   |   Workload  |
    |             |             |      &      |
    |             |             |    return   |
    |             |    relay   <-- telemetry  |
    |    commit  <--telemetry to|  to driver  |
    |telemetry to |  controller |             |
    | database &  |             |             |
    |display real-|             |             |
    | time stats  |             |             |
    |-------------|---------------------------|



Workload Generation Methodology
-------------------------------

Load is generated in the worker processes with boto3. This was to avoid having
to write all of the low-level S3 requests at the expense of losing the ability
to place timers deep in the request code (measure signature calculation time,
time to first byte, etc). Not really sure it matters, can revisit at some point
if desired.

MPUs

If multi-part uploads are specified in the workload configuration then the 
create_multipart_upload, put_part and complete_multipart_upload calls are 
measured as a single operation with the put_part operations conducted in 
serial (per worker process).


Sampling Methodology
--------------------

The worker processes collect data in 5 second samples by default. Bytes and
IOs are summed over the course of the sample and response times for individual
requests are appended to a list. The sample is sent to the controller process
where it is recorded in a database and real-time stats are displayed to the 
console.



Mixed Workloads
---------------

Mixed workloads are probability based. The workload indicates percentages 
for each operation type and each worker executes operations based on that
probability. If the workload indicates a 70% read and 30% write workload,
then each time an operation is executed there's a 70% chance it will be
a read and a 30% chance it will be a write. Precise distribution of IOs
is improved the longer the workload runs.


Command Line Arguments
======================

--verbose

    Dumps more output to the console, might be overwhelming (might not even work)

--name

    Name of the workload.

--profile

    It you have a boto3 profile in your home dir (~/.aws/credentials), this will credentials from the indicated profile name for creating and accessing S3 buckets.

--endpoint

    Name of the S3 endpoint to use. Slor will generally use path-style access to get to buckets.

--verify

    Verify HTTPS certs, defaults to "true". Set to "false" or supply a path to a CA bundle (bundle needs to be present on all driver hosts).

--region

    Specify region to use in the request. Used to construct the endpoint if you're running workloads on AWS S3 for some reason.

--access-key

    Specify the the access key credential.

--secret-key

    Specify the secret key credential.

--loads

    Specify the workloads you would like to run and in what order. Correct choices are: read, write, head, delete, mixed, sleep and blowout.

--mixed-profiles

    When specifying "mixed" as a workload, you'll need to define what that means. Mixed workload profiles are supplied as an array of JSON object. Since you can specify multiple workloads of the same type under "--loads" (e.g.: read,write,mixed,write,mixed), you may need more than one profile in the array. Here's an example profile:

        '[{"read": 6, "reread": 1, "write": 3, "overwrite": 1, "delete": 2, "head": 3}]'

    The value assigned to an operation indicates its "share" of the total operations. For instance the above example will work out to:

        read: 37.5%, reread: 6.25%, write: 18.75%, overwrite: 6.25, delete: 12.5, head: 18.75%

    Here's an example where 1 out of every 1000 operations is a HEAD.

        '[{"read": 999, "head": 1}]'

    If percentages are easier for you to work with, you can specify shares as percentages and that will work too (integers only). 

        '[{"read": 30, "write": 60, "head": 10}]'

    A "--loads" list with two "mixed" instances in it (one 70/30 read/write, and one 30/70 read/write) will look like this:

        '[{"read": 7, "reread": 3}, {"read": 3, "reread": 7}]'

--stage-time

    Time in seconds to run each stage. If you would like different lengths of time for each stage you may want to just issue separate generation commands with different lengths. You can reused data between runs with the "--save-readmap" flag.

--op-ceiling

    Used in tandem with "--stage-time" to determine the amount of data needed to be prepared for any read loads. Let's say you think there's no way your system will be able to sustain 5000 operations a second and that you want to run your stages for 300 seconds. Indicate "--stage-time 300 --iop-limit 5000" and slor will prepare 1.5 million objects ahead of any read workloads (pure read or mixed) to ensure you do not reread any data.

--prepare-objects

    If you prefer to directly specify the number of objects to prepare you can just indicate that here. Accepts suffixes (10M, 542K, etc).

--cachemem-size

    Indicate the expected page-cache + controller-cache capacity of the target storage cluster. This value is used by the "blowout" stage to write N bytes of data to the cluster (in 8MB objects). This is to overrun the system page and controller caches with garbage, forcing cold reads from disk during a subsequent read workload.

--sleep

    Used by the "sleep" workload. Indicate the number of seconds you want each "sleep" stage to last.

--bucket-count

    Indicate the number of buckets you would like to use in this test. Reads and writes will distribute evenly across buckets.

--bucket-prefix

    Bucket prefix name to use when creating buckets. For instance, if your bucket prefix is "slor-" and you indicate 4 for "--bucket-count" you'll get these 4 buckets: slor-0, slor-1, slor-2 and slor-3.

--use-existing-buckets

    SLoR will fail to start if a target benchmark bucket already exists. This is to prevent the accidental used of an existing operational bucket that might get polluted with benchmark garbage, or worse, cleaned-up removing all data. Since this tool will operation on versioned buckets it will also annihilate all old versions of anything in the bucket if --cleanup is specified. Very dangerous. Use this to override that protection. It'll be necessary if you want to operate on pre-created buckets (like if you want to test object locked buckets, policies or something).

--object-size

    The object size you would like to use in benchmarking. Accepts suffixes (KB, MB, GB, TB, EB). Also accepts ranges like "64KB-4MB." Indicating ranges will will results in random object sizes within the range (inclusive).

--random-from-pool

    Use random data from a pool of data rather than generate on-the-fly. This could
    be necessary if random data generation is using up too much CPU. Probably not,
    but the option is here anyway.

--compressible

    Make data compressible by the percentage you indicate. Currently this is pretty
    barbaric and will most likely get caught by data deduplication engines rather
    than data compression.

--get-range

    Use get-range size (or size range) rather than get of whole objects during read
    stages.

--mpu-size

    Writes are always performed in a single PUT operation regardless of size. If, however, you indicate a multi-part upload (MPU) size, writes will always be executed as multi-part uploads. Timing data will cover the MPU creation, PUT operations (in serial) and MPU completion as a single time-to-complete value - per worker process.
    MPU workloads are not parallelized across workers.

--versioning

    Creates versioned buckets at the start of workloads (or fails to start if bucket already exists), and stores version IDs for prepared data. It will cause read, reread, delete and head operations to be directed at object versions.

--driver-list

    Comma-delimited list of host/port numbers of drivers to attach to (can be IP/ports).

--processes-per-driver

    Indicate how many processes will spawn per driver. Defaults to 10.

--prepare-procs-per-driver

    Use a different processes count for the prepare stage.

--save-readmap

    Each workload requiring a "prepare" stage includes a "readmap" stage - during which a list of bucket/key values are computed before any data is written. If you pass "--save-readmap" a file name, it will save this key list in JSON format. This is useful if the workload you're executing does not have any DELETEs in it and you do not include the "--cleanup" flag. You can then use "--use-readmap" in subsequent
    workloads to reuse data.

--use-readmap

    Load bucket/key list from a saved readmap for use in a read workload. For reusing data written during a previous load.

--no-db

    Do not save workload timing data to a database on the controller host. This is useful if your workload is long-running and/or you have no intention of running an analysis on the database to extract detailed load statistics. The stats database can get very large and shouldn't be generated when running multi-hour loads.

--no-plot

    Do not draw graphical plots in the terminal during workloads. You may need to do
    this for UI performance reasons but probably not.
