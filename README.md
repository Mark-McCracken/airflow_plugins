# Airflow Plugins

Just drop the contents into your $AIRFLOW_HOME/plugins directory

Then in your DAG, import like so:

```python
from airflow.operators.dataflow_batch_plugin import StartDataflowPythonBatchJobOperator
from airflow.sensors.dataflow_batch_plugin import DataflowJobCompleteSensor
```
