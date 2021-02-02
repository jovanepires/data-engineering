from dags.stg_pipeline_tasks import task_list_sources
from dags.stg_pipeline_tasks import task_load_data 
from dags.stg_pipeline_tasks import task_rename_columns 
from repository import BaseRepository

def run():
    sources = task_list_sources()

    for source in sources:
        dataframe = task_load_data(source)
        dataframe = task_rename_columns(source, dataframe)
        repository = BaseRepository(source).connect_db()
        repository.insert(dataframe)

if __name__ == '__main__':
    run()