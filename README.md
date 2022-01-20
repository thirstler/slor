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
cross-compatible.


Installation
------------

Slor is not currently packaged so just download it, find "slor.py" and run it
where it is (this will likely change at some point).



NOTES TO SELF
=============

Mixed workload structure:

mixed = {
    "reads": 0,        # %
    "write": 0,        # %
    "delete": 0,       # %
    "head": 0,         # %
    "reread": 0,       # %
    "overwrite": 0,    # %
    "tag_write": 0,    # %
    "tag_read": 0,     # %
    "tag_config": {
        "key_len": 0,  # Range or value
        "value_len: 0  # Range or value
    }
}

