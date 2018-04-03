
from __future__ import print_function

import time

from pyspark import SparkContext
from datetime import datetime, timedelta
import pytz
import pandas as pd

from parallelm.mlops import mlops as mlops
from parallelm.mlops import StatCategory as st
from parallelm.mlops.stats.kpi_value import KpiValue
from parallelm.mlops.stats.multi_line_graph import MultiLineGraph


def _get_my_agents():
    """
    Return agent list of current ION node
    :return: Agent list used by current ION node
    """

    # Getting the first agent of ion component "0"
    agent_list = mlops.get_ion_component_agents(mlops.ion_node_id)
    if len(agent_list) == 0:
        print("Error - must have agents this ion component is running on")
        raise Exception("Agent list is empty")
    return agent_list


def test_stats_basic():
    # Adding multiple points (to see a graph in the ui), expecting each run to generate 8 points
    mlops.stat("stat1", 1.0, st.TIME_SERIES)
    mlops.stat("stat1", 3.0, st.TIME_SERIES)
    mlops.stat("stat1", 4.0, st.TIME_SERIES)
    mlops.stat("stat1", 2.0, st.TIME_SERIES)
    mlops.stat("stat1", 5.0, st.TIME_SERIES)
    mlops.stat("stat1", 6.0, st.TIME_SERIES)
    mlops.stat("stat1", 7.0, st.TIME_SERIES)
    mlops.stat("stat1", 8.0, st.TIME_SERIES)

    print("Done reporting statistics")
    time.sleep(10)

    print("MyION = {}".format(mlops.ion_id))
    now = datetime.utcnow()
    last_hour = (now - timedelta(hours=1))

    print("Hour before: {}".format(last_hour))
    print("Now:         {}".format(now))

    agent_list = _get_my_agents()

    df = mlops.get_stats(stat_name="stat1", ion_component=mlops.ion_node_id,
                         agent=agent_list[0].id, start_time=last_hour, end_time=now)
    print("Got stat1 statistic\n", df)


def test_multiline():
    print("Testing multiline")

    data = [[5, 15, 20], [55, 155, 255], [75, 175, 275]]
    columns = ["a", "b", "c"]
    expected_df = pd.DataFrame(data, columns=columns)
    stat_name = "stat-multi-line-test"
    # Multi line graphs
    for row in data:
        mlt = MultiLineGraph().name(stat_name).labels(columns).data(row)
        mlops.stat(mlt)

    time.sleep(10)
    agent_list = _get_my_agents()
    now = datetime.utcnow()
    last_hour = (now - timedelta(hours=1))

    df = mlops.get_stats(stat_name=stat_name, ion_component=mlops.ion_node_id,
                         agent=agent_list[0].id, start_time=last_hour, end_time=now)

    print("Got multiline df\n", df)
    df_tail = pd.DataFrame(df.tail(len(data))).reset_index(drop=True)
    print("df_tail:\n", df_tail)
    df_only_cols = df_tail[columns]
    print("Tail of multiline df + only relevant cols\n", df_only_cols)
    print("Expected:\n", expected_df)

    # We expect the tail (last rows) of the result df to be like the expected df.
    assert expected_df.equals(df_only_cols), "Expected multiline is not equal to obtained\n-------\n{}\n-------\n{}\n".format(
        expected_df, df_only_cols)


def test_kpi_basic():
    sec_now = int(time.time())
    sec_4h_ago = sec_now - (3600 * 4)
    kpi_window_start = sec_4h_ago
    val = 3.56
    nr_kpi_point = 10
    kpi_name = "test-kpi-1"
    kpi_window_end = kpi_window_start
    for i in range(nr_kpi_point):
        mlops.kpi(kpi_name, val, kpi_window_end, KpiValue.TIME_SEC)
        kpi_window_end += 1
        val += 1

    time.sleep(5)

    agent_list = _get_my_agents()

    kpi_datetime_start = datetime.utcfromtimestamp(kpi_window_start)
    kpi_datetime_end = datetime.utcfromtimestamp(kpi_window_end)

    print("datetime start: {}".format(kpi_datetime_start))
    print("datetime end:   {}".format(kpi_datetime_end))

    df = mlops.get_stats(stat_name=kpi_name, ion_component=mlops.ion_node_id,
                         agent=agent_list[0].id, start_time=kpi_datetime_start, end_time=kpi_datetime_end)
    print(df)
    if len(df) != nr_kpi_point:
        raise Exception("Got: {} kpi points, expecting: {}".format(len(df), nr_kpi_point))
