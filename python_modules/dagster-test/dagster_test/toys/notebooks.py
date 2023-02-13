from dagster import job
from dagstermill.factory import define_dagstermill_op

hello_world_notebook_op = define_dagstermill_op("hello_world_notebook_op", "hello_world.ipynb")


@job
def hello_world_notebook_job():
    hello_world_notebook_op()
