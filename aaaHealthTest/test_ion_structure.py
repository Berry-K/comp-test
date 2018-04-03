

from __future__ import print_function


from pyspark import SparkContext
from datetime import datetime, timedelta

from parallelm.mlops import mlops as mlops


def test_ion_structure():

    print("MyION = {}".format(mlops.ion_id))
    print("Components:")
    for c in mlops.ion_components:
        print(c)
    print("\n\n")
    # Getting the ION component object

    curr_comp_name = str(mlops.ion_node_id)
    curr_comp = mlops.get_ion_component(curr_comp_name)
    if curr_comp is None:
        raise Exception("Expecting {} component to exist in this ION".format(curr_comp_name))
    print("C0:\n{}".format(curr_comp))

    assert curr_comp.name == curr_comp_name

    # Getting the first agent of ion component "0"
    print("Getting component {} agents {}".format(curr_comp.name, type(curr_comp.name)))

    agent_list = mlops.get_ion_component_agents(curr_comp.name)

    if len(agent_list) == 0:
        print("Error - must have agents this ion component is running on")
        raise Exception("Agent list is empty")

    for agent in agent_list:
        if agent is None:
            raise Exception("Agent object obtained is None")
        print("Agent: {}".format(agent))
