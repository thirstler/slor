from slor.shared import *
from slor.driver import _slor_driver
from slor.stat_handler import *
import yaml, json

'''
def parse_workload(file):

    try:
        workload_f = yaml.safe_load(open(file, "r"))
    except Exception as e:
        print("can't read file ({}): {}".format(file, str(e)))
        return None
    if "workgroups" not in workload_f:
        print("no workgroups in the workload file")

    config = {"config_type": "advanced"}

    # Global items for use in workload definitions
    global_config_template = workload_f["global"]

    if "name" not in workload_f:
        config["name"] = "unnamed"

    for i, work in enumerate(workload_f["workgroups"]):

        # Place global items where not configured
        for item in global_config_template:
            if item not in work:
                work[item] = global_config_template[item]

        if "auto_prepare" in work:
            work["auto_prepare"] = parse_size(work["auto_prepare"])
        if "overrun" in work:
            work["overrun"] = parse_size(work["overrun"])
        if not "clean" in work:
            work["clean"] = False

        if not "name" in work:
            work["name"] = (
                global_config_template["name"] + "_{}".format(i)
                if "name" in global_config_template
                else str(i)
            )

        if "prefix_struct" in work:
            work["prefix_list"] = tuple(build_prefix_list(work["prefix_struct"]))
            work["prefix_placement_map"] = tuple(
                build_prefix_placement_map(work["prefix_list"])
            )

        for item in work["work"]:

            if "object_size" in item:
                item["object_size"] = parse_size_range(item["object_size"])
            if "key_length" in item:
                item["key_length"] = parse_size_range(item["key_length"])

        # Check workgroup:
        if not all(
            x in work
            for x in (
                "name",
                "access_key",
                "secret_key",
                "endpoint",
                "bucket_prefix",
                "bucket_count",
            )
        ):
            work["prefix_list"].clear()
            sys.stderr.write("missing minimal config item(s)\n")
            sys.exit(1)

    return workload_f


def build_prefix_placement_map(keylist):
    ratio_ttl = 0
    for key in keylist:
        ratio_ttl += key[1]

    prefix_pmap = []

    ki = 0
    while ki < len(keylist):
        for m in range(0, keylist[ki][1]):
            prefix_pmap.append(ki)
        ki += 1

    return prefix_pmap


def build_prefix_list(struct_def):

    keylist = _build_prefix_list(struct_def)

    mult = 1
    loopon = True
    while loopon:
        if any((x[1] * mult) < 100 for x in keylist):
            mult += 1
        else:
            break

    for key in keylist:
        key[1] = int(key[1] * mult)

    # Finally, tuple-fy
    for k in range(0, len(keylist)):
        keylist[k] = (keylist[k][0], keylist[k][1])

    return keylist


def _build_prefix_list(struct_def, prefix_key="", ratio_mult=1, delimiter="/"):

    keys = []
    for prefix_s in struct_def:
        prefix_s["key_length"] = parse_size_range(prefix_s["key_length"])
        for k in range(0, prefix_s["num_prefix"]):
            key = prefix_key + gen_key(key_desc=prefix_s["key_length"]) + delimiter
            keys.append([key, (prefix_s["ratio"] * ratio_mult)])
            if "prefix_struct" in prefix_s:
                keys += _build_prefix_list(
                    prefix_s["prefix_struct"],
                    prefix_key=key,
                    ratio_mult=(prefix_s["ratio"] / prefix_s["num_prefix"]),
                )
    return keys


def build_prefix_keys(struct_def):

    for prefix in struct_def:
        keylist = []
        # Generate keys
        prefix["key_length"] = parse_size_range(prefix["key_length"])
        for kc in range(0, int(prefix["num_prefix"])):
            key = gen_key(key_desc=prefix["key_length"]) + "/"
            keylist.append(key)
        prefix["keys"] = copy.copy(keylist)

        if "prefix_struct" in prefix:
            build_prefix_keys(prefix["prefix_struct"])
'''

'''
def parse_size_range(stringval):
    if type(stringval) == list or type(stringval) == tuple:
        return stringval  # this has already been processed
    if type(stringval) == int or type(stringval) == float:
        return (int(stringval), int(stringval), int(stringval))
    if not "-" in stringval:
        sz = parse_size(stringval)
        return (sz, sz, sz)
    else:
        vals = stringval.split("-")
        low = int(parse_size(vals[0].strip()))
        high = int(parse_size(vals[1].strip()))
        avg = (low + high) / 2
        return (low, high, avg)
'''


def parse_driver_list(stringval):
    hostlist = []
    for hostport in stringval.split(","):
        if ":" in hostport:
            host = hostport.split(":")[0]
            port = int(hostport.split(":")[1])
        else:
            host = hostport
            port = int(DEFAULT_DRIVER_PORT)
        hostlist.append({"host": host, "port": port})
    return hostlist

def parse_get_range(stringVal):
    if stringVal == None: return None

    items = stringVal.split("-")
    items[0] = int(parse_size(items[0]))
    if len(items) == 2:
        items[1] = int(parse_size(items[1]))

    return items

def generate_tasks(args):

    loads = list(args.loads.split(","))
    mix_prof_obj = {}
    config_supplements = {}

    for l in loads:
        if l not in LOAD_TYPES:
            sys.stderr.write('"{0}" is not a load option\n'.format(l))
            sys.exit(1)

    if "mixed" in loads:

        mix_prof_obj = json.loads(args.mixed_profiles)
        check_mixed_workloads(mix_prof_obj, loads)

    # Always happens:
    loads.insert(0, "init")

    # Create a readmap and add prep stage if needed
    if any(x in loads for x in ["read", "mixed", "head", "delete", "tag"]):
        loads.insert(1, "readmap")  # Will always be after init
        if not args.use_readmap:
            loads.insert(2, "prepare")  # Will always be after readmap


    if "blowout" in loads and args.cachemem_size == "0":
        try:
            while True:
                loads.remove("blowout") # Exception abuse
        except:
            pass

    # Add cleanup if specified
    if args.cleanup:
        loads.append("cleanup")

    if args.remove_buckets and "cleanup" not in loads:
        loads.append("cleanup")

    ##
    # Add supplemental config overrides
    if int(args.prepare_procs_per_driver) != -1 and  "prepare" in loads:
        if "prepare" not in config_supplements:
            config_supplements["prepare"] = {}
        config_supplements["prepare"]["processes"] = int(args.prepare_procs_per_driver)

    
    tasks = {"loadorder": loads, "mixed_profiles": mix_prof_obj, "config_supplements": config_supplements}

    return tasks


def basic_workload(args):
    pass


def verify_endpoint(endpoint):
    if endpoint[:7] == "http://" or endpoint[:8] == "https://":
        return endpoint
    else:
        return "http://" + endpoint


def check_mixed_workloads(mix_prof_obj, loads):
    # if loads.count("mixed") != len(mix_prof_obj):
    mcount = 0
    for w in loads:
        if w[:5] == "mixed":
            mcount += 1

    if mcount != len(mix_prof_obj):
        sys.stderr.write(
            "\nCONFIG ERROR: you have {} mixed load(s) queued, but {} mixed profile(s) defined\n\n".format(
                loads.count("mixed"), len(mix_prof_obj)
            )
        )
        sys.exit(1)


def classic_workload(args):
    
    # if no cmd line args, get from profile, then env (in that order)
    if not args.access_key and not args.secret_key:
        args.access_key, args.secret_key = get_keys(args.profile)

    # Must be AWS if no endpoint is given, to keep boto3 easy, we should
    # construct the AWS endpoint explicitly.
    if not args.endpoint:
        args.endpoint = "https://s3.{0}.amazonaws.com".format(args.region)

    tasks = generate_tasks(args)

    if args.prepare_objects:
        ttl_prepare_sz = int(parse_size(args.prepare_objects) * sizeRange(range_arg=args.object_size).avg)+1
        args.prepare_objects = int(parse_size(args.prepare_objects))
    else:
        ttl_prepare_sz = sizeRange(range_arg=args.object_size).avg * int(args.stage_time) * int(args.op_ceiling)
        args.prepare_objects = int(args.stage_time) * int(args.op_ceiling)


    # Create a working config from command line arguments
    root_config = {
        "name": args.name,
        "config_type": "basic",
        "verbose": args.verbose,
        "access_key": args.access_key,
        "secret_key": args.secret_key,
        "endpoint": verify_endpoint(args.endpoint),
        "verify": args.verify,
        "region": args.region,
        "key_sz": sizeRange(range_arg=args.key_length).serialize(),
        "sz_range": sizeRange(range_arg=args.object_size).serialize(),
        "random_from_pool": args.random_from_pool,
        "compressible": float(args.compressible)/100,
        "mpu_size": parse_size(args.mpu_size),
        "run_time": int(args.stage_time),
        "bucket_count": int(args.bucket_count),
        "bucket_prefix": args.bucket_prefix,
        "driver_list": parse_driver_list(args.driver_list),
        "sleeptime": float(args.sleep),
        "driver_proc": int(args.processes_per_driver),
        "ttl_sz_cache": parse_size(args.cachemem_size),
        "iop_limit": int(args.op_ceiling),
        "ttl_prepare_sz": ttl_prepare_sz,
        "tasks": tasks,
        "mixed_profiles": json.loads(args.mixed_profiles),
        "save_readmap": args.save_readmap,
        "use_readmap": args.use_readmap,
        "prepare_objects": args.prepare_objects,
        "key_prefix": args.key_prefix,
        "no_db": args.no_db,
        "no_plot": args.no_plot,
        "versioning": args.versioning,
        "remove_buckets": args.remove_buckets,
        "use_existing_buckets": args.use_existing_buckets,
        "get_range": sizeRange(range_arg=args.get_range).serialize() if args.get_range else None
    }
    return root_config
