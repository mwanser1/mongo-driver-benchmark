import copy
import multiprocessing as mp
import time

import pymongo


def worker(count):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["test"]
    mycol = mydb["python"]

    docs = [{"title": "Dune", "author": "Frank Herbert"}]
    for j in range(33):
        docs.append({"title": "I, Robot", "author": "Isaac Asimov"})
        docs.append({"title": "Foundation", "author": "Isaac Asimov"})
        docs.append({"title": "Brave New World", "author": "Aldous Huxley"})

    for i in range(6250):
        mycol.insert_many(copy.deepcopy(docs))

    myclient.close()
    return


if __name__ == '__main__':
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["test"]
    mycol = mydb["python"]

    mycol.drop()

    start = time.time()

    # Insert all docs using 8 threads
    processes = list()
    for i in range(2):
        p = mp.Process(target=worker, args=(i,))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    end = time.time()
    print(u'Insert time: ', end - start)

    # Now read all docs sequentially

    totalChars = 0
    start = time.time()
    cursor = mycol.find({"author": "Isaac Asimov"}, batch_size=1000)
    for doc in cursor:
        title = doc["title"]
        author = doc["author"]
        totalChars += len(title) + len(author)

    print(u'Total chars: ', totalChars)

    end = time.time()
    print(u'Fetch time: ', end - start)
