from __future__ import annotations

import logging
import os
from pathlib import Path

import pandas as pd
from aiogram import Bot, Dispatcher, F, Router, html
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import FSInputFile, Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

from gis_etl.data_processing import load_from_csv, transform_table
from gis_etl.arcgis_uploader import upload_dataframe
from gis_etl.google_sheets import load_google_sheet
from .bot_config import get_bot_config


logger = logging.getLogger(__name__)

router = Router()

# Просте in-memory сховище шляхів до останніх файлів по chat.id
USER_RESULTS: dict[int, str] = {}


@router.message(Command("start"))
async def cmd_start(message: Message):
    text = (
        "Привіт! Я бот для тестового завдання MagneticOne.\n\n"
        
        "Я допоможу обробити табличні дані та завантажити їх у картографічний шар ArcGIS Online.\n\n"
        
        "<b>Варіант 1: Напряму з Google Spreadsheets\n</b>"
        "Реалізовано командою /google_spreadsheets\n\n"
        
        "<b>Варіант 2: Надіслати вручну файл CSV/XLSX\n</b>"
        "1. Надішли мені файл CSV або XLSX як документ (іконка скріпки).\n"
        "2. Я поверну оброблений результат (CSV + XLSX).\n"
        "3️. Використай /upload_arcgis, щоб завантажити результат у Hosted Feature Layer.\n\n"
        "Докладний опис команд та контактів розробника дивись у /help."
    )
    await message.answer(text)


@router.message(Command("help"))
async def cmd_help(message: Message):
    text = (
        "<b>MagneticOne GIS ETL Bot</b>\n"
        "Бот для виконання тестового завдання ГІС-розробника:\n"
        "- перетворення табличних даних (CSV/XLSX) у нормалізований вигляд;\n"
        "- завантаження результату у Hosted Feature Layer в ArcGIS Online.\n\n"
        "<b>КОМАНДИ</b>\n"
        "/start – коротке вітальне повідомлення.\n"
        "/help – це повідомлення, опис можливостей та контакти.\n"
        "/google_spreadsheets – завантажити та обробити дані з Google Sheets.\n"
        "/upload_arcgis – завантажити останній оброблений файл у Hosted Feature Layer.\n\n"
        "<b>КОНТАКТИ</b>\n"
        "Telegram: @foff_pls\n"
        "Email: ogelcast@gmail.com\n"
        "Phone: +380973204030\n"
        "Git repository: https://github.com/foffpls/MagneticOne"
    )
    await message.answer(text, parse_mode=ParseMode.HTML)


async def _process_dataframe_source(message: Message, df, source_label: str) -> None:
    # Постійна директорія для результатів
    results_dir = Path("data") / "results"
    results_dir.mkdir(parents=True, exist_ok=True)

    stem = source_label
    csv_output_path = results_dir / f"{message.chat.id}_processed_{stem}.csv"
    xlsx_output_path = results_dir / f"{message.chat.id}_processed_{stem}.xlsx"

    # Перевірка наявності необхідних колонок
    required_columns = {"Дата", "Область", "Місто"}
    missing = required_columns - set(df.columns)
    if missing:
        await message.answer(
            "У надісланому файлі відсутні необхідні колонки.\n"
            f"Бракує: {', '.join(sorted(missing))}.\n"
            "Перевір, будь ласка, структуру таблиці згідно з ТЗ."
        )
        return

    transformed = transform_table(df)

    src_rows = len(df)
    out_rows = len(transformed)
    unique_cities = transformed["Місто"].nunique() if "Місто" in transformed.columns else 0
    unique_regions = transformed["Область"].nunique() if "Область" in transformed.columns else 0

    transformed.to_csv(csv_output_path, index=False)
    transformed.to_excel(xlsx_output_path, index=False, engine="openpyxl")

    USER_RESULTS[message.chat.id] = str(csv_output_path)

    summary_text = (
        "Готово! ✅ Дані оброблено згідно з ТЗ.\n\n"
        f"- Рядків у вихідному файлі: {src_rows}\n"
        f"- Рядків у результаті (точок): {out_rows}\n"
        f"- Унікальних міст: {unique_cities}\n"
        f"- Унікальних областей: {unique_regions}\n\n"
        "Я надіслав тобі оброблені файли (CSV та XLSX).\n"
        "Щоб завантажити результат у Hosted Feature Layer, скористайся командою /upload_arcgis."
    )

    await message.answer(summary_text)

    await message.answer_document(
        document=FSInputFile(str(csv_output_path), filename=csv_output_path.name),
        caption="Оброблений CSV-файл згідно з ТЗ (Частина 1).",
    )
    await message.answer_document(
        document=FSInputFile(str(xlsx_output_path), filename=xlsx_output_path.name),
        caption="Той самий результат у форматі Excel (XLSX) для ручної перевірки.",
    )


@router.message(F.document)
async def handle_csv(message: Message):
    document = message.document
    if not document:
        return

    filename = document.file_name or ""
    lower_name = filename.lower()
    if not (lower_name.endswith(".csv") or lower_name.endswith(".xlsx")):
        await message.answer("Будь ласка, надішли файл у форматі CSV або XLSX.")
        return

    await message.answer("Отримав файл, обробляю... Це може зайняти кілька секунд.")

    # Постійна директорія для результатів
    results_dir = Path("data") / "results"
    results_dir.mkdir(parents=True, exist_ok=True)

    stem = Path(filename).stem
    input_path = results_dir / f"{message.chat.id}_source_{stem}{Path(filename).suffix}"

    bot: Bot = message.bot
    file = await bot.get_file(document.file_id)
    await bot.download_file(file.file_path, destination=str(input_path))

    # Читаємо або як CSV, або як XLSX
    if lower_name.endswith(".csv"):
        df = load_from_csv(str(input_path))
    else:
        df = pd.read_excel(str(input_path))

    await _process_dataframe_source(message, df, stem)


@router.message(F.text & ~F.text.startswith("/"))
async def fallback_text(message: Message):
    await message.answer(
        "Вибачте, мене писали поспіхом!\n"
        "Нажаль, я не можу з вами поспілкуватись.\n\n"
        "Щоб працювати зі мною, надішли файл CSV/XLSX або скористайся командою /help.",
    )


@router.message(Command("upload_arcgis"))
async def cmd_upload_arcgis(message: Message):
    cfg = get_bot_config()
    last_path = USER_RESULTS.get(message.chat.id)

    if not last_path or not os.path.exists(last_path):
        await message.answer(
            "<b>Немає збереженого результату для завантаження!</b>\n"
            "Спочатку надішли файл CSV або XLSX, щоб я його обробив.\n"
            "Потім знову виклич /upload_arcgis\n\n"
            "Або ж завантажте дані напряму із Google Spreadsheets командою /google_spreadsheets",
        )
        return

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Підтвердити",
                    callback_data="confirm_upload",
                )
            ],
            [
                InlineKeyboardButton(
                    text="❌ Скасувати",
                    callback_data="cancel_upload",
                )
            ],
        ]
    )

    await message.answer(
        "УВАГА: будуть видалені всі попередні об'єкти в Hosted Feature Layer "
        "та завантажені нові дані з останнього обробленого CSV.\n\n"
        "Бажаєте продовжити завантаження?",
        reply_markup=keyboard,
    )


@router.callback_query(F.data == "confirm_upload")
async def cb_confirm_upload(callback: CallbackQuery):
    cfg = get_bot_config()
    chat_id = callback.message.chat.id if callback.message else callback.from_user.id
    last_path = USER_RESULTS.get(chat_id)

    if not last_path or not os.path.exists(last_path):
        await callback.message.edit_text("Не вдалося знайти підготовлений файл для завантаження.\n"
                                         "Будь ласка, надішли CSV/XLSX ще раз і повтори операцію.")
        return

    await callback.message.edit_text("Завантажую дані в ArcGIS Online, зачекай...")

    df = load_from_csv(last_path)
    try:
        upload_dataframe(df, clear_existing=True)
    except Exception as exc:
        logger.error("Помилка при завантаженні у ArcGIS: %s", exc, exc_info=True)
        await callback.message.edit_text("Сталася помилка при завантаженні даних у Hosted Feature Layer.\n"
                                        f"Технічні деталі: {exc}")
        return

    if cfg.arcgis_item_url:
        await callback.message.edit_text(
            "Дані успішно завантажено у Hosted Feature Layer.\n"
            f"Переглянути шар можна за цим {html.link('посиланням', cfg.arcgis_item_url)}!",
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
    else:
        await callback.message.answer(
            "Дані завантажено, але ARCGIS_ITEM_ID не налаштований, "
            "тому я не можу сформувати посилання на шар."
        )


@router.callback_query(F.data == "cancel_upload")
async def cb_cancel_upload(callback: CallbackQuery):
    await callback.message.edit_text("Операцію завантаження скасовано!\n"
                                     "Дані в Hosted Feature Layer не змінювались.")


@router.message(Command("google_spreadsheets"))
async def cmd_google_spreadsheets(message: Message):
    cfg = get_bot_config()
    if not cfg.google_sheet_url:
        await message.answer(
            "Не налаштовано адресу Google Sheets.\n"
            "Будь ласка, додай змінну GOOGLE_SHEET_URL у `.env` "
            "з посиланням на таблицю та перезапусти бота."
        )
        return

    await message.answer(
        "Завантажую дані з Google Sheets та обробляю їх згідно з ТЗ...\n"
        "Це може зайняти кілька секунд."
    )

    try:
        df = load_google_sheet(cfg.google_sheet_url)
    except Exception as exc:
        logger.error("Помилка при завантаженні Google Sheets: %s", exc, exc_info=True)
        await message.answer(
            "Не вдалося завантажити або прочитати дані з Google Sheets.\n"
            "Перевір, будь ласка, що таблиця доступна для перегляду за посиланням "
            "та що URL у змінній GOOGLE_SHEET_URL коректний.\n"
            f"Технічні деталі: {exc}"
        )
        return

    await _process_dataframe_source(message, df, "google_sheet")


def build_application():
    cfg = get_bot_config()
    if not cfg.token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN не налаштований в оточенні/.env")

    dp = Dispatcher()
    dp.include_router(router)
    return cfg, dp


def run_bot() -> None:
    import asyncio

    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=logging.INFO,
    )

    cfg, dp = build_application()
    bot = Bot(
        token=cfg.token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    async def main():
        await dp.start_polling(bot)

    asyncio.run(main())
