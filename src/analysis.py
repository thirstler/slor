import argparse
from slor_a import *

def run():
    parser = argparse.ArgumentParser(
        description="Slor (S3 Load Ruler) is a distributed load generation and benchmarking tool for S3 storage"
    )
    parser.add_argument("analysis")  # Make argparse happy
    parser.add_argument(
        "--input", required=True, help="database file to analyse"
    )
    parser.add_argument("--dump-csv", action="store_true", help="dump time-series data to stdout in CSV format")
    args = parser.parse_args()

    analysis = SlorAnalysis(args)
    
    if args.dump_csv:
        analysis.dump_csv()
    else:
        analysis.print_basic_stats()


