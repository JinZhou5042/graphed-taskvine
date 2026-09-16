"""Run a small graphed Plan through VineGraph local execution."""

from graphed.core.execution import Partition, Plan, Task

from taskvine_backend import TaskVineExecutor


def partition_size(partition, resources):
    del resources
    return partition.entry_stop - partition.entry_start


def add(left, right):
    return left + right


def zero():
    return 0


partitions = tuple(Task(key, Partition(f"input-{key}", "Events", 0, key + 1)) for key in range(4))
plan = Plan(process=partition_size, combine=add, empty=zero, tasks=partitions)

with TaskVineExecutor(local=True) as executor:
    result = executor.run(plan)

print(result.value)  # 10
