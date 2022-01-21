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

Complex workload structure:

{
    "prefix_struct": [
        {
            "num_prefix": INT,
            "key_length": INT,
            "ratio" INT,           # weight for determining probability of objects getting placed here on write
            "prefix_struc": [
                {
                    "num_prefix": INT,
                    "key_length": INT,
                    "ratio": INT,  # sub-ratio of parent
                    "prefix_struc": [ ... ]
                }
            ]
        }
    ],
    "work": [
        {
            "stream_name": arbitrary name, streams with the same name will share object operations with other streams (overwrite).
            "action": oneof(read, write, reread, overwrite, head, delete)
            "object_size": bytes or range,
            "key_length": length or range, in chars, for keys
            "ratio": weight number to be used relative to the other operations to determine occurrence probability for this operation,
        }
    ]
}

Example workload file contents:
{
    "name": "Demo Workload File"
    "access_key": "admin",             #
    "secret_key": "password",          #
    "endpoint": "http://localhost",    # 
    "verify_tls": False,               # Theses items can be located in the "workgroup" section as well
    "region": "us-east-1",             #
    "bucket_prefix": "bench-bucket-",  #
    "bucket_count": 8,                 #
    "prefix_struct": [                 # Prefix structure can be defined in the workgroup
        {
            "num_prefix": 20,
            "key_length": 20
            "ratio": 5,
            "prefix_struct": [
                {
                    "num_prefix": 25,
                    "key_length": 18,
                    "ratio": 1
                    "prefix_struct": [
                        {
                            "num_prefix": 12,
                            "key_length": 18,
                            "ratio": 1
                        }
                    ]
                }
            ]
        },
        {
            "num_prefix": 20,
            "key_length": 14
            "ratio": 1,
            "prefix_struct": [
                "num_prefix": 12,
                "key_length": 20,
                "ratio": 1,
                "prefix_struct": [
                    {
                        "num_prefix": 25,
                        "key_length": 18,
                        "ratio": 1
                    }
                ]
            ]
        },
    ],
    "workgroups": [
        {
            "time": 3600,
            "auto_prepare": "1000G" # use the write ratios to create this amount of data ahead of time
            "auto_overrun": "1.7T"  # Write a lot of garbage after prepare stage to overrun page/controller caches
            "prefix_struct": {}
            "work": [
                {
                    "stream_name": "indexes",
                    "action": "write",
                    "object_size": "1K-15K",
                    "key_length" "4-12",
                    "ratio": 50
                },
                {
                    "stream_name": "indexes",
                    "action": "read",
                    "ratio": 50
                },
                {
                    "stream_name": "indexes",
                    "action": "reread",
                    "ratio": 10
                },
                {
                    "stream_name": "indexes",
                    "action": "overwrite",
                    "object_size": "1K-15K",
                    "key_length" "4-12",
                    "ratio": 10
                },
                {
                    "stream_name": "indexes",
                    "action": "delete",
                    "ratio": 2
                },
                {
                    "stream_name": "blobs",
                    "action": "write",
                    "object_size": "4M-22M",
                    "key_length" "18",
                    "ratio": 500
                },
                {
                    "stream_name": "blobs",
                    "action": "read",
                    "ratio": 5000
                },
                {
                    "stream_name": "blobs",
                    "action": "reread",
                    "ratio": 10000
                },
                {
                    "stream_name": "blobs",
                    "action": "head",
                    "ratio": 5000
                },
                {
                    "stream_name": "blobs",
                    "action": "delete",
                    "ratio": 100
                }
            ]
        }
    ]
}



