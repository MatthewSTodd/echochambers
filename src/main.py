import argparse

from preprocessing.ops_on_raw_data import ops_on_corona, ops_on_vac
from preprocessing.ops_build_graph import graph_ops
from community.community_detection import start_community_detection
from preprocessing.topic_modelling import add_topic
from controversy_detection.start_controversy_detection import start_detection
from link_prediction.start_link_prediction import start_link_opt
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter


import sys

def preprocessing_operation(parse_raw):
    # Create the final data file if the raw_data hasn't yet been parsed
    if parse_raw:
        ops_on_corona()
    # TODO validate that ops_on_vac does nothing for corona data
    # ops_on_vac()
    graph_ops()
    print("PREPROCESSING DONE")
    print("")

def community_detection():
    start_community_detection()

def controversy_detection():
    start_detection()

def link_prediction():
    start_link_opt()

if __name__ == '__main__':
    parser = ArgumentParser(description="Main Parser", formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("-r", "--raw", action="store_true", help="Use raw unprocessed twitter files")
    args = parser.parse_args()
    preprocessing_operation(parse_raw=args.raw)
    community_detection()
    controversy_detection()
    link_prediction()
