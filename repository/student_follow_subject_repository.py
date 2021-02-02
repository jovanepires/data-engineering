from repository.base_repository import BaseRepository

class StudentFollowSubjectRepository(BaseRepository):

    def __init__(self):
        super(StudentFollowSubjectRepository, self).__init__(table="student_follow_subject")