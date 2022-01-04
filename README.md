Slor
====

Slor (S3 Load Ruler) is an easy-to-use benchmarking and load generation tool
for S3 storage systems. It operates on the same controller/driver arrangement used by other load-generation systems:

    controller ___ Driver1
                \_ Driver2
                \_ Driver3
                \_ etc...

It runs wherever Python3 does and minimizes dependencies where possible. The
ones that make the most of a fuss seem to be boto3 an psutil. They can both
be "pip installed" if necessary but CentOS/RedHat w/EPEL has them in as
RPM packages as well.

It is currently not packages so just download and run where it is (this
will likely change at some point).

Getting Started
---------------

It's assumed that you have an S3 service you want to benchmark or torture. 
It's also assumes you have an endpoint, keys and that know what region
you'll be using (if needed). I'm also thinking you have a host that you're
going to use for load generation.

With that out of the way, start a driver on your load generation host:

    ./slor.py driver

Great, in another window on the load generation host, let's looks at the
"--help" output for the controller option:

    ./slor.py controller --help
    -h, --help            show this help message and exit
    --verbose             verbose output
    --name NAME           name for this workload/benchmark
    --bucket-prefix BUCKET_PREFIX
                            prefix to use when creating buckets (defaults to
                            "slor-")
    --profile PROFILE
    --endpoint ENDPOINT
    --verify VERIFY       verify HTTPS certs, defaults to "true"; set to "false"
                            or a path to a CA bundle (bundle needs to be present
                            on all driver hosts
    --region REGION
    --access-key ACCESS_KEY
    --secret-key SECRET_KEY
    --stage-time STAGE_TIME
                            how long each load stage should run (default: 300
                            seconds)
    --key-length KEY_LENGTH
                            key length(s) to use, can be single number or range
                            (e.g. 10,50) - defaults to 40
    --object-size OBJECT_SIZE
                            object size to use; accepts values with common
                            suffixes (1MB, 1MiB) and ranges (1KB-12MiB) - defaults
                            to 1MB
    --driver-list DRIVER_LIST
                            comma-delimited list of driver hosts running "slor
                            driver" processes (in host:port format); 9256 is
                            assumed if port is excluded
    --processes-per-driver PROCESSES_PER_DRIVER
                            number of simultaneous processes per driver host;
                            drivers * processes_per_driver will equal total
                            processes (defaults to 1)
    --cachemem-size CACHEMEM_SIZE
                            total amount of memory available in the target cluster
                            for page cache and read caches; post-prepare stage
                            will write this value to overwhelm caches for a cold
                            read stage
    --iop-limit IOP_LIMIT
                            maximum expected IOP/s value that you can expect to
                            hit given the workload; needed to determine the size
                            of the prepare data given the load run-time
    --loads LOADS         specify the loads you want to run; any (or all) of
                            read, write, delete, head, mixed
    --sleep SLEEP         sleeptime between workloads
    --mix-profile MIX_PROFILE
                            profile of mixed load percentages in JASON format, eg:
                            '{"read": 60, "write": 25, "delete": 5, "head": 10 }'
    --bucket-count BUCKET_COUNT
                            number of buckets to distribute over, defaults to '1'

Yikes.  

