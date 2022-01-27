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
    args = parser.parse_args()

    analysis = SlorAnalysis(args.input)


