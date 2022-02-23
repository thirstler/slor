Slor
====

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

Slor is not currently packaged so just download it, find "slor.py" and run it
where it is (this will likely change at some point).


Running a Workload
==================

You can easily start a workload using the "controller" command. Minimally you
need an accessKey/secretKey and an endpoint to use. For example:

    ./slor controller --access-key user --secret-key password --endpoint http://127.0.0.1

A driver process will automatically launch a default benchmark workload with the 
following settings:

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

This is a perfectly good way to start but you'll likely


