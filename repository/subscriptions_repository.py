from repository.base_repository import BaseRepository

class SubscriptionsRepository(BaseRepository):

    def __init__(self):
        super(SubscriptionsRepository, self).__init__(table="subscriptions")