from nd_api_v2.models.incremental_queue import IncrementalQueue

# def create_incremental_queue(queue_name: str) -> IncrementalQueue:
#     return IncrementalQueue.objects.create(queue_name=queue_name)

# def insert_into_incremental_queue(queue: IncrementalQueue, start_value: int, end_value: int) -> None:
#     queue.nd_auto_increment_start_value = start_value
#     queue.nd_auto_increment_end_value = end_value
#     queue.save()
