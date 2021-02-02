from repository.base_repository import BaseRepository

class UniversitiesRepository(BaseRepository):

    def __init__(self):
        super(UniversitiesRepository, self).__init__(table="universities")