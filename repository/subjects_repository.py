from repository.base_repository import BaseRepository

class SubjectsRepository(BaseRepository):

    def __init__(self):
        super(SubjectsRepository, self).__init__(table="subjects")