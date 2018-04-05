from __future__ import print_function

from pyspark import SparkContext
import sys
import argparse

from parallelm.mlops import mlops as pm
from parallelm.mlops import mlops as mlops
from parallelm.mlops.e2e_tests.health_node.runner import run_mlops_tests
import parallelm.mlops.e2e_tests.health_node


def parse_args():
    print("Got args: {}".format(sys.argv))
    parser = argparse.ArgumentParser()

    parser.add_argument("--test-name", default=None, help="Provide a test to run - if not provided all tests are run")
    options = parser.parse_args()

    return options


def main():
    options = parse_args()

    sc = SparkContext(appName="health-test")

    pm.init(sc)
    print("MLOps testing")
    print("My ION Node id:   {}".format(mlops.get_current_node().id))
    print("My ION Node name: {}".format(mlops.get_current_node().name))

    run_mlops_tests(package_to_scan= parallelm.mlops.e2e_tests.health_node, test_to_run=options.test_name)

    sc.stop()
    pm.done()


if __name__ == "__main__":
    main()
