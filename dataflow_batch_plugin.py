from airflow.plugins_manager import AirflowPlugin
from dataflow_batch.dataflow_python_batch import DataflowJobCompleteSensor, StartDataflowPythonBatchJobOperator


class DataflowBatchPlugin(AirflowPlugin):
    name = 'dataflow_batch_plugin'
    operators = [StartDataflowPythonBatchJobOperator]
    sensors = [DataflowJobCompleteSensor]
