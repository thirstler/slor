Slor
====

SLOR ▏▎▍▌▊▉█▇▇▆▆▆▅▅▅▅▄▄▄▄▄▃▃▃▃▃▃▂▂▂▂▂▂▂▂▁▁▁▁▁▁▁▁▁▁▁▁▁▁ 

Slor (S3 Load Ruler) is an easy-to-use benchmarking and load generation tool
for S3 storage systems. 

What does it do?
----------------

Slor can be used to benchmark the performance capabilities of S3 storage 
systems. It will measure throughput, bandwidth and processing times of
several basic S3 operations (read, write, head, delete, overwrite, reread
and tagging) in discrete and mixed workload configurations. 

How does it do it?
------------------
It operates on the same controller/driver arrangement used by other
load-generation systems:

    Controller ___ Driver1
                \_ Driver2
                \_ Driver3
                \_ etc...

You start workload servers, or drivers, on the systems you want to use for
load generation. Then, you will references these servers when starting
workloads with slor via a "controller" process, which is simply responsible
for sending workloads to driver processes and then collecting performance
data returned by the driver processes.

Where does it run?
------------------

It should run anywhere Python3 runs, though it is more tested on POSIX type
systems. That said, Windows should work as well. The only limiting factor
is that the drivers and controller process all need to be on the same OS
type (Linux, Max, Windows, *BSD). This is because it makes extensive use of
the multiprocessing module which has handlers for simplifying for inter-process
communication. These handlers automatically pickle data in formats that aren't
cross-compatible. Even different python builds on different Linux distributions
can be incompatible. It should be possible to fix the picking problem later. 


Installation
------------

Slor is not currently packaged so just download it, find "slor" and run it
where it is (this will likely change at some point).


Running a Workload
------------------

You can easily start a workload using the "controller" command. Minimally you
need an accessKey/secretKey and an endpoint to use. For example:

    ./slor controller --access-key user --secret-key password --endpoint http://123.45.67.89

A driver process will automatically launch and a default benchmark workload
will be sent to it with the following default workload configuraiton and settings:

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

This is a perfectly good way to start but you'll likely want to benchmark 
different file sizes, I/O thread counts, workload mixtures, bucket counts,
whatever. You can specify the stages in the workload with the --loads flag.

    ./slor controller --access-key user \
    --secret-key password \
    --endpoint http://123.45.67.89 \
    --loads read,write,cleanup

This will run only a read and write workload with a clean-up stage at the end
that will remove all objects and then delete the bucket.

Let's add a mixed workload (that isn't the default). Mixed workload accepts
the following operation classes along with a percentage for how often that
operation will appear in the stage (percentages must add-up to 100):

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
    ----mixed-profiles '[{"read": 50, "reread": 10, "write": 20, "overwrite": 5, "delete": 5, "head": 10}]'

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
    ----mixed-profiles '[
        {"read": 50, "reread": 10, "write": 20, "overwrite": 5, "delete": 5, "head": 10},
        {"read": 10, "reread": 2, "write": 68, "overwrite": 5, "delete": 5, "head": 10}
    ]'

If you're benchmarking spinning disks you might need to overrun page cache
so that you're testing cold reads from disk rather than data from memory.
This is done with a "blowout" stage. In order to use this effectively you 
need to know how much cache you need to overrun before you can force the 
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
but you cannot change the paramaters for the individually. For instance
you can't, in the same workload command, specify a different object size
for the write stage than for the read stage - or separate cachemem-sizes
for blowout stages - or different run-times for each stage. For that you
simply need to run with separate loads with appropriate settings.

Distributed Jobs
----------------

So far we've been using the built-in load-generator that's launch with 
slor when you don't specify drivers to attach to. On separate load generation
hosts start the drive with the.... driver sub-command.

    ./slor driver

This launches a driver process (does not daemonize) that attaches to the
default port (9256). Do this on one or more hosts and then launch work
loads from a controller specifying the hostnames of the hosts running the
drivers:

    ./slor controller --access-key user \
    --secret-key password \
    --endpoint http://127.0.0.1 \
    --cachemem-size 3.072T \
    --loads write,blowout,read,cleanup \
    --object-size 256K-5M \
    --driver-list loadgen1,loadgen2,loadgen3

Now the load will launch and distribute over each host equally. The default
process count of 10 is per-driver, so this will launch a total of 30 threads
of IO total.

If you're trying to generate serious load you'll want to increase that per-
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
trying to realistically assess platform performance you don't want to re-read
the same data over and over again. This means you need to have _enough_ data
to sustain the IO rate for the length of time the benchmark takes to run. 
Honestly, you can't really know that until you benchmark the system, can you?
This is a hard fact of life. You can probably make a pretty good guess though.
What are the network limitations? What are the IO limitations of the metadata
layer? What do you already know? I know this system couldn't possibly take 
more than 10,000 IO/s of reads at 256KB so let's assume a ceiling of 
12,000 IO/s and call it a day. That would be 3.07 GB/s of bandwidth.

You can tell SLOR the amount of data to auto-prepare with the "--iop-limit"
flag. It will use this in conjunction with the "--stage-time" flag which 
specifies how long each stage will run (defaults to 300 seconds). Let's start
a workload that can sustain 12,000 operations per second of reads for 15
minutes using a 256KB object size:

    ./slor controller --access-key user \
    --secret-key password \
    --endpoint http://127.0.0.1 \
    --cachemem-size 3.072T \
    --loads write,blowout,read,cleanup \
    --object-size 256K \
    --driver-list loadgen1,loadgen2,loadgen3,loadgen4 \
    --processes-per-driver 64
    --stage-time 900 \
    --iop-limit 12000

This will prepare 2.7TB (10.8 million objects) of 256K files anticipating 
that reads will not exceed 12000 operations a second. It will blow-out
the page cache before the read stage, run each timed stage for 15 min and
clean up when it's done (the "blowout" stage takes as long as it takes).


Other Options
-------------

An other thing you can specify is key length. This can be done one of two
ways (and both at the same time): by using a prefix that's appended to each
key and by specify the key length itself. This can be useful if you think
metadata length is affecting performance. Let's specify a run using a
key-length of ~120 random character along with a static prefix appended
to each key:


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

It's painful to see data go to waste. If you're doing a lot of read-testing
and _not_ deleting data as part for the workloads, you can reuse data with 
the "--save-readmap" flag.

SLOR pre-generates all of the keys it's going to use for prepared data (this is
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
that any object size or key information to specify in this command line is
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
    --loads blowout,read,mixed,delete,cleanup \
    --mixed-profiles '[{"read": 50, "write": 25, "head": 25}]
    --sleep-time 60 \
    --use-readmap /tmp/readmap.json

Of course, in the above workload, there's a delete and clean-up stage which 
makes the saved readmap file useless after this run. I hope you're happy.

Internals
=========

Drivers and Communication
-------------------------

Workload Generation Methodology
-------------------------------

Sampling Methodology
--------------------

Mixed Workloads
---------------

















