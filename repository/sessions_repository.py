from repository.base_repository import BaseRepository

class SessionsRepository(BaseRepository):

    def __init__(self):
        super(SessionsRepository, self).__init__(table="sessions")