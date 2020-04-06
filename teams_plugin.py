from airflow.plugins_manager import AirflowPlugin
from teams.teams_operator import TeamsOperator


class TeamsPlugin(AirflowPlugin):
    name = 'teams_plugin'
    operators = [TeamsOperator]
