import os
import asyncio
from pathlib import Path
from typing import Any, Dict, List, Optional

from aiogram import Bot
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.types import FSInputFile
from dotenv import load_dotenv
from PyPDF2 import PdfReader, PdfWriter
from rq import get_current_job


load_dotenv()

TOKEN = os.getenv("BOT_TOKEN")
PROXY_URL = os.getenv("PROXY_URL") or None
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "temp_files/results"))


def update_progress(job, stage: str, progress: int, message: str) -> None:
    if job is None:
        return

    job.meta["stage"] = stage
    job.meta["progress"] = progress
    job.meta["message"] = message
    job.save_meta()


async def send_result_to_user(
    chat_id: int,
    output_files: Optional[List[str]] = None,
    error: Optional[str] = None
) -> None:
    if not TOKEN:
        raise RuntimeError("Не найден BOT_TOKEN в .env")

    session = AiohttpSession(proxy=PROXY_URL) if PROXY_URL else AiohttpSession()
    bot = Bot(token=TOKEN, session=session)

    try:
        if error:
            await bot.send_message(chat_id, f"Ошибка при обработке файла:\n{error}")
            return

        output_files = output_files or []

        await bot.send_message(
            chat_id,
            f"Обработка завершена.\n"
            f"Файл разделен на {len(output_files)} частей."
        )

        for file_path in output_files:
            if os.path.exists(file_path):
                await bot.send_document(chat_id, FSInputFile(file_path))

    finally:
        await bot.session.close()


def process_pdf_task(request_contract: Dict[str, Any]) -> Dict[str, Any]:
    """
    Фоновая задача RQ.
    Получает контракт от bot.py, разбивает PDF-файл на части и отправляет результат пользователю.
    """

    job = get_current_job()
    chat_id = None
    output_files: List[str] = []

    try:
        task_id = request_contract["task_id"]
        chat_id = int(request_contract["chat_id"])
        file_path = Path(request_contract["file_path"])
        file_name = request_contract.get("file_name", file_path.name)
        chunk_size = int(request_contract.get("chunk_size", 5))
        contract_version = request_contract.get("contract_version", "1.0")

        if chunk_size <= 0:
            raise ValueError("chunk_size должен быть больше 0")

        if not file_path.exists():
            raise FileNotFoundError(f"Исходный файл не найден: {file_path}")

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        print(f"\n[STEP 1] Worker начал обработку задачи {task_id}")
        print(f"[CONTRACT] Версия контракта: {contract_version}")
        print(f"[FILE] Исходный файл: {file_path}")

        update_progress(
            job,
            stage="started",
            progress=5,
            message="Worker начал обработку файла"
        )

        reader = PdfReader(str(file_path))
        total_pages = len(reader.pages)

        if total_pages == 0:
            raise ValueError("PDF-файл не содержит страниц")

        print(f"[STEP 2] Файл прочитан. Всего страниц: {total_pages}")

        update_progress(
            job,
            stage="reading",
            progress=10,
            message=f"Файл прочитан. Всего страниц: {total_pages}"
        )

        total_chunks = (total_pages + chunk_size - 1) // chunk_size
        base_name = Path(file_name).stem

        for chunk_number, start_page in enumerate(range(0, total_pages, chunk_size), start=1):
            writer = PdfWriter()
            end_page = min(start_page + chunk_size, total_pages)

            for page_index in range(start_page, end_page):
                writer.add_page(reader.pages[page_index])

            output_file = OUTPUT_DIR / f"{task_id}_part_{chunk_number}_{base_name}.pdf"

            with open(output_file, "wb") as file_out:
                writer.write(file_out)

            output_files.append(str(output_file))

            progress = int(10 + (chunk_number / total_chunks) * 80)

            print(
                f"[STEP 3] Сформирована часть {chunk_number} "
                f"(стр. {start_page + 1}-{end_page})"
            )

            update_progress(
                job,
                stage="processing",
                progress=progress,
                message=f"Сформирована часть {chunk_number} из {total_chunks}"
            )

        print("[STEP 4] Отправка результата пользователю...")

        update_progress(
            job,
            stage="sending",
            progress=95,
            message="Отправка результата пользователю"
        )

        asyncio.run(send_result_to_user(chat_id, output_files=output_files))

        update_progress(
            job,
            stage="finished",
            progress=100,
            message="Задача успешно завершена"
        )

        print(f"[FINISH] Задача {task_id} успешно выполнена\n")

        return {
            "task_id": task_id,
            "state": "finished",
            "chunks": output_files,
            "total_pages": total_pages,
            "chunk_size": chunk_size,
            "contract_version": contract_version,
        }

    except Exception as error:
        error_text = str(error)

        print(f"[ERROR] Ошибка в задаче RQ: {error_text}")

        update_progress(
            job,
            stage="failed",
            progress=100,
            message=error_text
        )

        if chat_id is not None:
            try:
                asyncio.run(send_result_to_user(chat_id, error=error_text))
            except Exception as send_error:
                print(f"[ERROR] Не удалось отправить сообщение об ошибке: {send_error}")

        raise