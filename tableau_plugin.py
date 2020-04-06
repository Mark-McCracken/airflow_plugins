from airflow.plugins_manager import AirflowPlugin
from tableau.refresh_datasource_operator import RefreshDatasourceOperator


class TableauPlugin(AirflowPlugin):
    name = 'tableau_plugin'
    operators = [RefreshDatasourceOperator]
