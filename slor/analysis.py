import argparse
import sqlite3
from slor_a import *

def run():
    parser = argparse.ArgumentParser(
        description="Slor (S3 Load Ruler) is a distributed load generation and benchmarking tool for S3 storage"
    )
    parser.add_argument("analysis")  # Make argparse happy
    parser.add_argument(
        "--input", required=True, help="database file to analyse"
    )
    parser.add_argument("--csv-out", default=None, help="CSV output file with report and time-series data")
    args = parser.parse_args()

    analysis = SlorAnalysis(args)

    # Always show basic summary
    analysis.print_basic_stats()
    #analysis.get_all_csv()
    #if args.csv_out:
    #    analysis.get_all_csv()


