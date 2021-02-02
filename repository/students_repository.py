from repository.base_repository import BaseRepository

class StudentsRepository(BaseRepository):

    def __init__(self):
        super(StudentsRepository, self).__init__(table="students")