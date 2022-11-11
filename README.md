# mongodb-queue

A light-weight queue implementation with PyMongo.

Tested with Python 3.10 and PyMongo 4.3.2.

## Installation

```shell
pip install mongodb-queue
```

## How it works
```python
from mongodb_queue import Queue
queue = Queue(MONGODB_URL)
queue.add('Hello, World!')
row = queue.get()
queue.ping(row['ack'])
queue.ack(row['ack'])
queue.clean()
```


## Credits

This project is a python implementation of [chilts/mongodb-queue](https://github.com/chilts/mongodb-queue) with some improvements.
