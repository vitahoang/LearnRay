import time

import ray

database = [
    "Learning", "Ray",
    "Flexible", "Distributed", "Python", "for", "Machine", "Learning"
]


def retrieve(item):
    time.sleep(item / 10.)
    return item, database[item]


def print_runtime(input_data, start_time):
    print(f'Runtime: {time.time() - start_time:.2f} seconds, data:')
    print(*input_data, sep="\n")


db_object_ref = ray.put(database)


@ray.remote
def retrieve_task(item, db):
    time.sleep(item / 10.)
    return item, db[item]


# start = time.time()
# data = [retrieve(item) for item in range(8)]
# print_runtime(data, start)

start = time.time()
object_references = [
    retrieve_task.remote(item, db_object_ref) for item in range(8)]
data = ray.get(object_references)
print_runtime(data, start)

all_data = []

while len(object_references) > 0:
    finished, object_references = ray.wait(
        object_references, num_returns=2, timeout=7.0
    )
    data = ray.get(finished)
    print_runtime(data, start)
    all_data.extend(data)

print(all_data)


# Running a follow-up task that depends on another Ray task
@ray.remote
def follow_up_task(retrieve_result):
    original_item, _ = retrieve_result
    follow_up_result = retrieve(original_item + 1)
    return retrieve_result, follow_up_result


retrieve_refs = [retrieve_task.remote(item, db_object_ref) for item in
                 [0, 2, 4, 6]]
follow_up_refs = [follow_up_task.remote(ref) for ref in retrieve_refs]


# For that, Ray has the concept of actors. Actors allow you to run stateful
# computations on your cluster. They can also communicate between each other.
@ray.remote
class DataTracker:

    def __init__(self): self._counts = 0

    def increment(self): self._counts += 1

    def counts(self): return self._counts


@ray.remote
def retrieve_tracker_task(item, tracker, db):
    time.sleep(item / 10.)
    tracker.increment.remote()
    return item, db[item]


tracker = DataTracker.remote()
object_references = [
    retrieve_tracker_task.remote(item, tracker, db_object_ref) for item in
    range(8)
]
data = ray.get(object_references)
print(data)
print(ray.get(tracker.counts.remote()))
