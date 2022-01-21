import yaml, json
from shared import *

def parse_workload(file):
    workloads = []
    try:
        workload_f = yaml.safe_load(open(file, "r"))
    except Exception as e:
        print("can't read file ({}): {}".format(file, str(e)))
        return None
    if "workgroups" not in workload_f:
        print("no workgroups in the workload file")

    global_config_template = {}
    stage_chain = []
    for item in ("name", "access_key", "secret_key", "endpoint", "region", "bucket_prefix", "bucket_count", "verify_tls", "prefix_struct"):
        if item in workload_f:
            global_config_template[item] = workload_f[item]

    if "name" not in workload_f:
        global_config_template["name"] = "unnamed"

    for i, work in enumerate(workload_f["workgroups"]):
        work.update(global_config_template)
        
        work["prefix_list"] = build_prefix_list(work["prefix_struct"])
        
        # Normalize work item ratios into percentages
        mult = 0
        for item in work["work"]:
            mult += item["ratio"]
        mult = 1/mult

        for item in work["work"]:
            item["ratio"] *= mult
            if "object_size" in item:
                item["object_size"] = item["object_size"].split("-")
                if len(item["object_size"]) == 1:
                    item["object_size"].append(item["object_size"][0])
                item["object_size"][0] = parse_size(item["object_size"][0])
                item["object_size"][1] = parse_size(item["object_size"][1])

            if "key_length" in item:
                item["key_length"] = item["key_length"].split("-")
                if len(item["key_length"]) == 1:
                    item["key_length"].append(item["key_length"][0])
                item["key_length"][0] = int(item["key_length"][0])
                item["key_length"][1] = int(item["key_length"][1])

        if "auto_prepare" in work:
            work["auto_prepare"] = parse_size(work["auto_prepare"])
        if "overrun" in work:
            work["overrun"] = parse_size(work["overrun"])
        if not "clean" in work:
            work["clean"] = False

        if not "name" in work:
            work["name"] =  global_config_template["name"] + "_{}".format(i) if "name" in global_config_template else str(i)

        # Check workgroup:
        if not all(x in work for x in ("name", "access_key", "secret_key", "endpoint", "bucket_prefix", "bucket_count")):
            work["prefix_list"].clear()
            print(json.dumps(work))
            sys.stderr.write("missing minimal config item(s)\n")


        workloads.append(work)

    return workloads


def build_prefix_list(struct_def):
    keylist = _build_prefix_list(struct_def)

    # Normalize ratios into percentages
    mult = 0
    for k, r in keylist:
        mult += r
    mult = 1/mult

    for i in range(0, len(keylist)):
        keylist[i][1] *= mult
    
    return keylist

    

def _build_prefix_list(struct_def, parent="", ratio_mult=1.0):
    keylist = []
    for prefix in struct_def:
        for kc in range(0, int(prefix["num_prefix"])):
            keylen = [prefix["key_length"],] if type(prefix["key_length"]) == int else prefix["key_length"].split("-")
            if len(keylen) == 1:
                keylen.append(keylen[0])
            key = parent + gen_key(key_desc=(int(keylen[0]), int(keylen[1]))) + "/"
            keylist.append([key, (int(prefix["ratio"]) * ratio_mult)])
            if "prefix_struct" in prefix:
                keylist.pop(-1)
                keylist += _build_prefix_list(prefix["prefix_struct"], parent=key, ratio_mult=float(prefix["ratio"])/int(prefix["num_prefix"]))
    
    return keylist


