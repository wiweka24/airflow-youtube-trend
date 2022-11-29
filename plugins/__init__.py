from airflow.plugins_manager import AirflowPlugin
import operators

# Defining the plugin class
class YoutubePlugin(AirflowPlugin):
    name = "youtubeplugin"
    operators = [
        operators.extractYtApiOperator,
        operators.transformYtApiOperator,
        operators.loadYtApiOperator
    ]