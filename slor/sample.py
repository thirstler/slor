import time
from typing import List
from xmlrpc.client import Boolean
import json

class perfSample:

    driver_id = None
    process_id = None
    count_target = None
    time_target = None
    global_io_count = 0
    global_io_time = 0
    window_start = None
    window_end = None
    sample_seq = None
    operations = None
    final = False
    ttl = 0


    def __init__(self, driver_id=None, process_id=None, count_target=None, time_target=None, start=False, sample_seq=0, from_json=None, start_io_count= None):
        if time_target and count_target:
            return False
        if from_json:
            # All imputs are ignore if from_json is used
            self.operations = {}
            self.from_json(from_json)

        else:
            self.operations = {}
            self.final = False
            self.ttl = 0
            self.driver_id = driver_id
            self.process_id = process_id
            self.count_target = count_target
            self.time_target = time_target
            self.sample_seq = sample_seq
            if start: self.window_start = time.time()
            if start_io_count:
                self.global_io_count = start_io_count

    def __del__(self):
        pass

    def percent_complete(self):
        if self.count_target:
            #print("{} / {} = {}".format(self.global_io_count, self.count_target, (self.global_io_count/self.count_target)))
            return self.global_io_count/self.count_target
        return None


    def init_global_sample(self, target=None, time_target=None):
        
        driver_id = None
        process_id = None
        count_target = target
        time_target = time_target
        global_io_count = 0
        window_start = None
        window_end = None
        sample_seq = 0
        operations = {}
        final = False


    def get_operations(self):
        operation_labels = []
        for op in self.operations:
            operation_labels.append(op)
        return operation_labels


    def merge(self, sample):
        # driver_id:       not merged
        # process_id:      not merged
        # count_target:    set on init_global_sample()
        # time_target:     set on init_global_sample()
        # window_start:    not merged
        # window_end:      not merged
        self.sample_seq += 1    # overloaded var for merging
        for op in sample.operations:
            if op not in self.operations:
                self.add_operation_class(op)
            self.add_ios(op, sample.get_metric(metric="ios", opclass=op))
            self.add_bytes(op, sample.get_metric(metric="bytes", opclass=op))
            self.add_failures(op, sample.get_metric(metric="failures", opclass=op))
            self.add_resp_time(op, sample.get_metric(metric="iotime", opclass=op))

        # Overwrite global_io_count to benchmark counter
        self.global_io_count += sample.global_io_count


    def from_json(self, from_json):
        self.driver_id = from_json["driver_id"]
        self.process_id = from_json["process_id"]
        self.global_io_count = from_json["global_io_count"]
        self.global_io_time = from_json["global_io_time"]
        self.window_start = from_json["window_start"]
        self.window_end = from_json["window_end"]
        self.sample_seq = from_json["sample_seq"]
        self.operations = from_json["operations"]
        if "count_target" in from_json:
            self.count_target = from_json["count_target"]
        elif "time_target" in from_json:
            self.time_target = from_json["time_target"]
        if "final" in from_json:
            self.final = from_json["final"]


    def dump_json(self):
        serialized_sample = {
            "driver_id": self.driver_id,
            "process_id": self.process_id,
            "global_io_count": self.global_io_count,
            "global_io_time": self.global_io_time,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "sample_seq": self.sample_seq,
            "operations": self.operations
        }
        if self.count_target:
            serialized_sample["count_target"] = self.count_target
        elif self.time_target:
            serialized_sample["time_target"] = self.time_target

        if self.final:
            serialized_sample["finale"] = True

        return json.dumps(serialized_sample)

    def start(self, start_time=None):
        if start_time:
            self.window_start = start_time
        else:
            self.window_start = time.time()

    def stop(self, stop_time=None):
        if stop_time:
            self.window_end = stop_time
        else:
            self.window_end = time.time()

    def add_operation_class(self, opclass:str, label=None) -> None:
        if not label:
            label = opclass
        if opclass not in self.operations:
            self.operations[label] = {
                "opclass": opclass,
                "ios": 0,
                "bytes": 0,
                "failures": 0,
                "iotime": []
            }

    def add_ios(self, opclass:str, value:int=1) -> None:
        if opclass not in self.operations:
            self.add_operation_class(opclass)
        self.operations[opclass]["ios"] += value
        self.global_io_count +=  value

    def add_bytes(self, opclass:str, value:int=0) -> None:
        if opclass not in self.operations:
            self.add_operation_class(opclass)
        self.operations[opclass]["bytes"] += value

    def add_failures(self, opclass:str, value:int=0) -> None:
        if opclass not in self.operations:
            self.add_operation_class(opclass)
        self.operations[opclass]["failures"] += value

    def add_resp_time(self, opclass:str, value) -> None:
        if opclass not in self.operations:
            self.add_operation_class(opclass)
        if type(value) == float:
            self.operations[opclass]["iotime"].append(value)
            self.global_io_time += value
        if type(value) == list:
            self.operations[opclass]["iotime"] += value
            self.global_io_time += sum(value)

    def _get_metric(self, metric:str, opclass:str):
        try:
            return self.operations[opclass][metric]
        except KeyError:
            return None

    def get_metric(self, metric:str, opclass:str=None):
        val = 0
        if opclass == None:
            for o in self.get_operations():
                val += self._get_metric(metric, o)
        else:
            val = self._get_metric(metric, opclass)
        return val

    def _get_rate(self, metric:str, opclass:str) -> float:
        if not self.window_start:
            return None
        try:
            return self.operations[opclass][metric]/self.walltime()
        except KeyError:
            return None

    def get_rate(self, metric:str, opclass:str=None) -> float:
        rate = 0
        if opclass == None:
            for op in self.get_operations():
                rate += self._get_rate(metric, op)
        else:
            rate = self._get_rate(metric, opclass)
        return rate

    def get_resp_avg(self, opclass:str) -> float:
        iottl = 0
        for r in self.operations[opclass]["iotime"]:
            iottl += r
        return iottl/len(self.operations[opclass]["iotime"])

    def get_workload_ios(self):
        io_ttl = 0
        for op in self.operations:
            io_ttl += self.get_metric("ios", op)
        return io_ttl

    def get_workload_io_rate(self):
        ttl = self.get_workload_ios()
        ttl /= self.walltime()
        return ttl

    def walltime(self):
        if not self.window_end:
            end_ref = time.time()
        else:
            end_ref = self.window_end
        return end_ref - self.window_start

    def _iotime(self, opclass:str):
        if opclass not in self.get_operations():
            return 0
        iotime = float()
        time_list = self.get_metric("iotime", opclass)

        if time_list == None:
            return 0

        for t in time_list:
            iotime += t
        return iotime

    def iotime(self, opclass:str=None):
        iotime = float()

        if opclass == None:
            for o in self.get_operations():
                iotime += self._iotime(o)
        else:
            iotime = self._iotime(opclass)

        return iotime
