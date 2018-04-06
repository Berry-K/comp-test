
from __future__ import print_function

from pyspark import SparkContext
from datetime import datetime, timedelta

from parallelm.mlops import mlops as mlops


def test_models():
    # Getting models:
    now = datetime.utcnow()
    last_hour = (now - timedelta(hours=1))

    print("MODEL-TEST")
    models = mlops.get_models(start_time=last_hour, end_time=now)
    print("models-df:\n", models)
    expected_model_data = "model-content-1234"

    print("Got {} models".format(len(models)))
    if len(models) >= 1:
        model_id = models.iloc[0]['id']
        print("\n\nDownloading first model: {} {}\n".format(model_id, type(model_id)))
        model_data = mlops.download_model(model_id)

        print("Model data: [{}]".format(model_data))
        assert model_data == expected_model_data

    models = mlops.get_models(start_time=last_hour, end_time=now, download=True)
    if len(models) >= 1:
        model_data = models.iloc[0]['data']
        assert model_data == expected_model_data
