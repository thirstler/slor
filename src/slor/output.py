from slor.shared import *

def config_text(config):
        
    cft_text = "CONFIGURATION:\n"
    cft_text += "\n"
    cft_text += "Workload name:      {0}\n".format(config["name"])
    cft_text += "User key:           {0}\n".format(config["access_key"])
    cft_text += "Target endpoint:    {0}\n".format(config["endpoint"])
    cft_text += "Stage run time:     {0}s\n".format(config["run_time"])
    if config["save_readmap"]:
        cft_text += "Saving readmap:     {0}\n".format(config["save_readmap"])
    if config["use_readmap"]:
        cft_text += "Using readmap:      {0}\n".format(config["use_readmap"])
    else:
        
        # calculate memory needed for bytepool per driver process
        driver_mem = (config["sz_range"]["high"]*2) * int(config["driver_proc"])

        cft_text += "Object size(s):     {0}\n".format(
            human_readable(config["sz_range"]["low"], precision=0)
            if config["sz_range"]["low"] == config["sz_range"]["high"]
            else "low: {0}, high: {1} (avg: {2})".format(
                human_readable(config["sz_range"]["low"], precision=0),
                human_readable(config["sz_range"]["high"], precision=0),
                human_readable(config["sz_range"]["avg"], precision=0),
            )
        )
        cft_text += "Req driver mem:     {0}\n".format(human_readable(driver_mem))
        if config["get_range"]:
            cft_text += "Get range size(s):  {0}\n".format(
                human_readable(config["get_range"]["low"], precision=0)
                if config["get_range"]["low"] == config["get_range"]["high"]
                else "low: {0}, high: {1} (avg: {2})".format(
                    human_readable(config["get_range"]["low"], precision=0),
                    human_readable(config["get_range"]["high"], precision=0),
                    human_readable(config["get_range"]["avg"], precision=0),
                )
            )
        cft_text += "Key length(s):      {0}\n".format(
            config["key_sz"]["low"]
            if config["key_sz"]["low"] == config["key_sz"]["high"]
            else "low: {0}, high:  {1} (avg: {2})".format(
                config["key_sz"]["low"],
                config["key_sz"]["high"],
                config["key_sz"]["avg"],
            )
        )
        cft_text += "Prepared objects:   {0} (readmap length)\n".format(
            human_readable(config["prepare_objects"], print_units="ops")
        )
        if not config["no_db"]:
            cft_text += "Database file:      {0} (base file name)\n".format("{}{}.db".format(POSIX_DB_TMP, config["name"]))
        cft_text += "Upper IO limit:     {0}\n".format(config["iop_limit"])
        cft_text += "Bucket prefix:      {0}\n".format(config["bucket_prefix"])
        cft_text += "Num buckets:        {0}\n".format(config["bucket_count"])
        cft_text += "Versioning enabled: {0}\n".format(str(config["versioning"]))
        cft_text += "Prepared data size: {0}\n".format(
            human_readable(config["ttl_prepare_sz"]),
            ((int(config["ttl_prepare_sz"]) / DEFAULT_CACHE_OVERRUN_OBJ) + 1),
            human_readable(DEFAULT_CACHE_OVERRUN_OBJ),
        )

    cft_text += "Driver processes:   {0}\n".format(len(config["driver_list"]))
    cft_text += "Procs per driver:   {0} ({1} worker processes total)\n".format(
        config["driver_proc"],
        (int(config["driver_proc"]) * len(config["driver_list"])),
    )
    if config["ttl_sz_cache"] > 0:
        cft_text += "Cache overrun size: {0} ({1} x {2} objects)\n".format(
            human_readable(config["ttl_sz_cache"]),
            (int(config["ttl_sz_cache"] / DEFAULT_CACHE_OVERRUN_OBJ) + 1),
            human_readable(DEFAULT_CACHE_OVERRUN_OBJ),
        )
    cft_text += "Stages:\n"
    stagecount = 0
    mixed_count = 0

    for i, stage in enumerate(config["tasks"]["loadorder"]):
        stage = (
            stage[: stage.find(":")] if ":" in stage else stage
        )  # Strip label information
        stagecount += 1
        duration = (
            config["sleeptime"]
            if stage == "sleep"
            else config["run_time"]
        )

        if stage == "mixed":
            mixed_prof = config["tasks"]["mixed_profiles"][mixed_count]
            mixed_perc = mixed_ratio_perc(mixed_prof)
            mixed_count += 1
            cft_text += "                    {0}: {1} - perc: ".format(stagecount, stage)
            for j, m in enumerate(mixed_perc):
                cft_text += "{0}:{1:.2f}%".format(m, mixed_perc[m]*100)
                if (j + 1) < len(mixed_perc):
                    cft_text += ", "
            cft_text += " ({} seconds)\n".format(duration)
        elif stage == "readmap":
            cft_text += "{} {}: {}\n".format(
                " " * 19,
                stagecount,
                "readmap - generate keys for use during read operations",
            )
        elif stage == "init":
            cft_text += "{} {}: {}\n".format(
                " " * 19, stagecount, "init - create needed buckets/config"
            )
        elif stage == "prepare":
            cft_text += "{} {}: {}\n".format(
                " " * 19,
                stagecount,
                "prepare - write objects needed for read operations",
            )
        elif stage == "blowout":
            cft_text += "{} {}: {}\n".format(
                " " * 19, stagecount, "blowout - overrun page cache"
            )
        elif stage == "read":
            cft_text += "{} {}: {}\n".format(
                " " * 19,
                stagecount,
                "read - pure GET workload ({} seconds)".format(duration),
            )
        elif stage == "write":
            cft_text += "{} {}: {}\n".format(
                " " * 19,
                stagecount,
                "write - pure PUT workload ({} seconds)".format(duration),
            )
        elif stage == "head":
            cft_text += "{} {}: {}\n".format(
                " " * 19,
                stagecount,
                "head - pure HEAD workload ({} seconds)".format(duration),
            )
        elif stage == "delete":
            cft_text += "{} {}: {}\n".format(
                " " * 19,
                stagecount,
                "delete - pure DELETE workload ({} seconds)".format(duration),
            )
        elif stage == "tag":
            cft_text += "{} {}: {}\n".format(
                " " * 19,
                stagecount,
                "tag - pure tagging (metadata) workload ({} seconds)".format(
                    duration
                ),
            )
        elif stage == "cleanup":
            cft_text += "{} {}: {}\n".format(
                " " * 19, stagecount, "cleanup - remove all objects{}".format(" (and buckets)" if config["remove_buckets"] else "")
            )
        elif stage[:5] == "sleep":
            cft_text += "{} {}: {}\n".format(
                " " * 19, stagecount, "sleep for {} seconds".format(duration)
            )

    return cft_text


def top_box(newline=True):
    sys.stdout.write("\u250C{0}".format("\u2500" * (os.get_terminal_size().columns - 1)))
    if newline:
        sys.stdout.write("\n")
    sys.stdout.flush()

def box_line(text, newline=False):
    sys.stdout.write("\u2502 {}".format(text))
    if newline:
        sys.stdout.write("\n")
    sys.stdout.flush()

def bottom_box(newline=True):
    sys.stdout.write("\u2514{0}".format("\u2500" * (os.get_terminal_size().columns - 1)))
    if newline:
        sys.stdout.write("\n")
    sys.stdout.flush()


def box_text(text):
    text_lines = text.split("\n")
    top_box()
    for line in text_lines:
        print("\u2502 " + line)
    bottom_box()

def indent(text, indent=1):

    text_lines = text.split("\n")
    for line in text_lines:
        sys.stdout.write(" "*indent + line + "\n")
    sys.stdout.flush()
    

def format_row(content:list, newline=False, replace=False, padding=0) -> str:
    """
    Content format:
    [
        (text, width, term_chars, justification),
        etc...
    ]
    example:
    [
        ("text content 1", 15, "\033[94m", ">"),
        ("more content", 8, "", "<")
    ]
    """
    row_text = ""
    if replace:
        row_text += "\r"
    for i, item in enumerate(content):
        format_str = item[2]+"{:"+item[3]+str(item[1])+"}"+"\033[0m"
        row_text += format_str.format(item[0])
        row_text += " "*padding
        
    if newline:
        row_text += "\n"

    return(row_text.rstrip(" "))
    

    

