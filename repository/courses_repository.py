from repository.base_repository import BaseRepository

class CoursesRepository(BaseRepository):

    def __init__(self):
        super(CoursesRepository, self).__init__(table="courses")