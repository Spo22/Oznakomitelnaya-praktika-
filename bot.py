import os
import sys
import uuid
import asyncio
from pathlib import Path

from aiogram import Bot, Dispatcher, F, types
from aiogram.client.session.aiohttp import AiohttpSession
from dotenv import load_dotenv
from redis import Redis
from rq import Queue
from rq.job import Job
from rq.exceptions import NoSuchJobError

from pipeline import process_pdf_task


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)

load_dotenv()

TOKEN = os.getenv("BOT_TOKEN")
PROXY_URL = os.getenv("PROXY_URL") or None
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
UPLOAD_DIR = Path(os.getenv("UPLOAD_DIR", "temp_files"))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "5"))

if not TOKEN:
    raise RuntimeError("Не найден BOT_TOKEN в .env")

UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

session = AiohttpSession(proxy=PROXY_URL) if PROXY_URL else AiohttpSession()
bot = Bot(token=TOKEN, session=session)
dp = Dispatcher()

redis_conn = Redis.from_url(REDIS_URL)
queue = Queue("pdf_tasks", connection=redis_conn)


@dp.message(F.text == "/start")
async def cmd_start(message: types.Message):
    user_name = message.from_user.full_name if message.from_user else "пользователь"
    print(f"[START] Пользователь {user_name} запустил бота")

    await message.answer(
        "Здравствуйте! Пришлите PDF-файл, и я разделю его на части по 5 страниц.\n\n"
        "После загрузки файла я отправлю ID задачи.\n"
        "Проверить статус можно командой:\n"
        "/status ID_задачи"
    )


@dp.message(F.text.startswith("/status"))
async def cmd_status(message: types.Message):
    parts = message.text.split(maxsplit=1)

    if len(parts) < 2:
        await message.answer("Укажите ID задачи. Пример: /status 123abc")
        return

    task_id = parts[1].strip()

    try:
        job = Job.fetch(task_id, connection=redis_conn)
    except NoSuchJobError:
        await message.answer("Задача с таким ID не найдена.")
        return

    status = job.get_status()
    status_text = status.value if hasattr(status, "value") else str(status)

    progress = job.meta.get("progress", 0)
    stage = job.meta.get("stage", "нет данных")
    message_text = job.meta.get("message", "")

    await message.answer(
        f"Статус задачи: {status_text}\n"
        f"Этап: {stage}\n"
        f"Прогресс: {progress}%\n"
        f"{message_text}"
    )


@dp.message(F.document)
async def handle_pdf(message: types.Message):
    user_name = message.from_user.full_name if message.from_user else "пользователь"
    file_name = message.document.file_name or "document.pdf"

    print(f"\n[RECEIVE] Получен файл от пользователя: {user_name}")
    print(f"[CHECK] Имя файла: {file_name}")

    if not file_name.lower().endswith(".pdf"):
        print(f"[REJECT] Отказ: формат файла не PDF")
        await message.answer("Пожалуйста, отправьте PDF-файл.")
        return

    task_id = uuid.uuid4().hex
    safe_file_name = Path(file_name).name
    file_path = UPLOAD_DIR / f"{task_id}_{safe_file_name}"

    print("[ACCEPT] Формат подтвержден. Начинаю загрузку файла...")

    file_info = await bot.get_file(message.document.file_id)
    await bot.download_file(file_info.file_path, destination=str(file_path))

    print(f"[STORAGE] Файл сохранен: {file_path}")

    request_contract = {
        "task_id": task_id,
        "chat_id": message.chat.id,
        "file_path": str(file_path),
        "file_name": safe_file_name,
        "chunk_size": CHUNK_SIZE,
        "contract_version": "1.0",
    }

    job = queue.enqueue(
        process_pdf_task,
        request_contract,
        job_id=task_id,
        job_timeout=600,
        result_ttl=86400,
        failure_ttl=86400,
    )

    job.meta["stage"] = "queued"
    job.meta["progress"] = 0
    job.meta["message"] = "Задача поставлена в очередь Redis/RQ"
    job.save_meta()

    print(f"[QUEUE] Задача добавлена в очередь. ID: {job.id}")

    await message.answer(
        f"Файл принят и добавлен в очередь обработки.\n"
        f"ID задачи: `{job.id}`\n\n"
        f"Проверить статус можно командой:\n"
        f"/status {job.id}",
        parse_mode="Markdown"
    )


async def main():
    print("--- СИСТЕМА ЗАПУЩЕНА ---")
    print(f"Redis: {REDIS_URL}")
    print(f"Прокси: {PROXY_URL}")

    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("\n--- СИСТЕМА ОСТАНОВЛЕНА ---")