import os

from dotenv import load_dotenv
from redis import Redis
from rq import Queue, SimpleWorker


load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

redis_conn = Redis.from_url(REDIS_URL)
queue = Queue("pdf_tasks", connection=redis_conn)


if __name__ == "__main__":
    print("--- SIMPLE WORKER ЗАПУЩЕН ---")
    print(f"Redis: {REDIS_URL}")
    print("Очередь: pdf_tasks")

    worker = SimpleWorker([queue], connection=redis_conn)
    worker.work()