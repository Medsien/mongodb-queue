from datetime import datetime, timedelta
from uuid import uuid4

from pymongo import ASCENDING, MongoClient, ReturnDocument


class QueueError(Exception):
    pass


class Queue:
    VISIBLE = "visible"
    PROCESSED = "processed"
    ACK = "ack"
    DATA = "data"
    TRIES = "tries"

    def __init__(
        self,
        uri,
        database="default",
        collection="queue",
        visibility=60 * 10,
        delay=0,
        dead_queue=False,
        max_retries=5,
    ):
        """
        URI: MongoDB URI format
        Visibility: If you don't ack a row within the visibility window, it is placed back in the queue.
        Delay: After a row has been added, it becomes available for retrieval after delay seconds.
        Dead Queue: Rows that have been retried over max_retries will be pushed to the dead queue.
        """
        self.client = MongoClient(uri)
        self.database = self.client[database]
        self.collection = self.database[collection]
        self.visibility = visibility
        self.delay = delay
        if dead_queue:
            self.dead_queue = Queue(uri, database=database, collection=f"dead_{collection}")
            self.max_retries = max_retries
        else:
            self.dead_queue = None
        self.check_indices()

    def check_indices(self):
        indexes = self.collection.index_information()
        if f"{self.VISIBLE}_1" not in indexes:
            self.collection.create_index([(self.VISIBLE, ASCENDING)])
        if f"{self.PROCESSED}_1" not in indexes:
            self.collection.create_index([(self.PROCESSED, ASCENDING)])
        if f"{self.ACK}_1" not in indexes:
            self.collection.create_index([(self.ACK, ASCENDING)], unique=True, sparse=True)

    def _uuid(self):
        return str(uuid4().int)[:12]

    def _utc_now(self, delay=0):
        return (datetime.utcnow() + timedelta(seconds=delay)).isoformat()

    @property
    def pending_query(self):
        return {self.PROCESSED: None, self.VISIBLE: {"$lte": self._utc_now()}}

    def ack_query(self, ack):
        return {self.PROCESSED: None, self.VISIBLE: {"$gt": self._utc_now()}, self.ACK: ack}

    def add(self, data):
        if not data:
            raise QueueError("Cannot add empty data to queue")
        if not isinstance(data, list):
            data = [data]
        visible = self._utc_now(self.delay)
        return self.collection.insert_many([{self.DATA: row, self.VISIBLE: visible} for row in data])

    def get(self):
        filter = self.pending_query
        update = {
            "$inc": {self.TRIES: 1},
            "$set": {self.ACK: self._uuid(), self.VISIBLE: self._utc_now(self.visibility)},
        }
        sort = [("_id", ASCENDING)]
        row = self.collection.find_one_and_update(filter, update, sort=sort, return_document=ReturnDocument.AFTER)
        if row and self.dead_queue:
            tries = row.get(self.TRIES, 0)
            if tries > self.max_retries:
                self.dead_queue.add(row.get(self.DATA))
                self.ack(row.get(self.ACK))
                row = self.get()
        return row

    def ping(self, ack):
        filter = self.ack_query(ack)
        update = {"$set": {self.VISIBLE: self._utc_now(self.visibility)}}
        return self.collection.find_one_and_update(filter, update, return_document=ReturnDocument.AFTER)

    def ack(self, ack):
        filter = self.ack_query(ack)
        update = {"$set": {self.PROCESSED: self._utc_now()}}
        return self.collection.find_one_and_update(filter, update, return_document=ReturnDocument.AFTER)

    def clean(self, all=False):
        """
        Clean all processed rows from the queue.
        If all is True, removes all rows regardless of whether they have been processed or not.
        """
        query = {}
        if not all:
            query[self.PROCESSED] = {"$exists": True}
        return self.collection.delete_many(query)

    @property
    def total(self):  # all rows
        return self.collection.count_documents({})

    @property
    def pending(self):  # rows that are waiting to be processed
        return self.collection.count_documents(self.pending_query)

    @property
    def processing(self):  # rows that are in flight
        return self.collection.count_documents(
            {
                self.PROCESSED: None,
                self.VISIBLE: {"$gt": self._utc_now()},
                self.ACK: {"$exists": True},
            }
        )

    @property
    def processed(self):  # rows that are processed
        return self.collection.count_documents({self.PROCESSED: {"$exists": True}})
