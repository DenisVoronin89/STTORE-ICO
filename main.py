import asyncio
import os
from fastapi import FastAPI, HTTPException, Query
from starlette.status import HTTP_403_FORBIDDEN
from typing import List, Union
from datetime import date
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

from logger import get_logger
from dotenv import load_dotenv
from database import init_db
from views import fetch_and_store_exchange_rate, calculate_monthly_stats

logger = get_logger()
load_dotenv()

AUTH_TOKEN = os.getenv("AUTH_TOKEN")

ENABLE_DOCS = os.getenv("ENABLE_DOCS", "false").lower() == "true"

app = FastAPI(
    title="Exchange Rate Service",
    docs_url="/docs" if ENABLE_DOCS else None,
    redoc_url="/redoc" if ENABLE_DOCS else None,
    openapi_url="/openapi.json" if ENABLE_DOCS else None
)

scheduler = AsyncIOScheduler()


async def scheduled_task():
    try:
        logger.info("Запуск запланированной задачи получения курса")
        await fetch_and_store_exchange_rate()
        logger.info("Задача выполнена успешно")
    except Exception as e:
        logger.error(f"Ошибка в запланированной задаче: {repr(e)}")


async def scheduled_monthly_stats_task():
    try:
        logger.info("Запуск запланированной задачи расчёта статистики за месяц")
        await calculate_monthly_stats()
        logger.info("Статистика за месяц успешно рассчитана")
    except Exception as e:
        logger.error(f"Ошибка в задаче расчёта статистики: {repr(e)}")


@app.on_event("startup")
async def startup_event():
    logger.info("Запуск приложения: инициализация БД")
    await init_db()
    logger.info("Приложение успешно запущено. Соединение с базой данных установлено.")

    scheduler.add_job(
        scheduled_task,
        trigger=IntervalTrigger(hours=24),
        id="fetch_exchange_rate_job",
        replace_existing=True
    )

    # Запускать в 00:00 в последний день каждого месяца
    scheduler.add_job(
        scheduled_monthly_stats_task,
        trigger=CronTrigger(hour=0, minute=0, day='last'),
        id="calculate_monthly_stats_job",
        replace_existing=True
    )

    scheduler.start()
    logger.info("Планировщик запущен, задача будет выполняться каждые сутки")


@app.on_event("shutdown")
async def shutdown_event():
    scheduler.shutdown()
    logger.info("Планировщик остановлен")


# Эндпоинт для получения курса покупки-продажи токена по контракту для первого графика
@app.get("/exchange_rates")
async def exchange_rates_endpoint(
    start_date: Union[str, date] = Query(..., description="Дата начала, формат YYYY-MM-DD"),
    end_date: Union[str, date] = Query(..., description="Дата конца, формат YYYY-MM-DD"),
) -> List[dict]:
    """
    Возвращает список курсов обмена за указанный диапазон дат.
    Просто вызывает get_exchange_rates_range и возвращает результат.
    """
    try:
        rates = await get_exchange_rates_range(start_date, end_date)
        return rates
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при получении курсов: {e}")


# Эндпоинт для добавления информации о транзакции.
@app.post("/transaction")
async def add_transaction(
    transaction_hash: str,
    amount: float,
    auth_token: str
):
    """
    Добавляет запись о новой транзакции в базу данных после проверки авторизационного токена.

    :param transaction_hash: Хэш транзакции (уникальный идентификатор в блокчейне)
    :param amount: Сумма перевода токенов по данной транзакции
    :param auth_token: Авторизационный токен для защиты от внешнего доступа
    :return: JSON с полем 'status': 'ok' при успешном сохранении
    :raises HTTPException 403: если передан неверный токен
    :raises HTTPException 400: если транзакция уже существует
    :raises HTTPException 500: при других ошибках сохранения
    """
    if auth_token != AUTH_TOKEN:
        raise HTTPException(status_code=HTTP_403_FORBIDDEN, detail="Неверный токен доступа")

    try:
        async with get_db_session() as session:
            session.add(Transaction(transaction_hash=transaction_hash, amount=amount))
            await session.commit()
            return {"status": "ok"}
    except IntegrityError:
        raise HTTPException(status_code=400, detail="Transaction already exists")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при сохранении транзакции: {e}")


# Эндпоинт для получения суммы транзакций по контракту (помесячно) для второго графика
@app.get("/monthly_stats")
async def get_monthly_stats(
    from_year: int = Query(..., alias="from_year"),
    from_month: int = Query(..., alias="from_month"),
    to_year: int = Query(..., alias="to_year"),
    to_month: int = Query(..., alias="to_month"),
):
    """
    Возвращает статистику по каждому месяцу в указанном диапазоне (включительно).
    Если статистика за месяц отсутствует — заполняется нулями.

    :param from_year: Год начала диапазона
    :param from_month: Месяц начала диапазона
    :param to_year: Год конца диапазона
    :param to_month: Месяц конца диапазона
    :return: Словарь вида {'YYYY-MM': {total_amount, total_count, all_time_count}}
    :raises HTTPException 400: если диапазон некорректный
    """
    if (to_year, to_month) < (from_year, from_month):
        raise HTTPException(status_code=400, detail="Некорректный диапазон")

    # Сформировать список (год, месяц) включительно
    months = []
    year, month = from_year, from_month
    while (year, month) <= (to_year, to_month):
        months.append((year, month))
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1

    async with get_db_session() as session:
        result = await session.execute(
            select(MonthlyStats).where(
                tuple_(MonthlyStats.year, MonthlyStats.month).in_(months)
            )
        )
        stats = result.scalars().all()

        # мапа {(year, month): stat}
        stats_map = {(s.year, s.month): s for s in stats}

        response = OrderedDict()
        for year, month in months:
            key = f"{year:04d}-{month:02d}"
            stat = stats_map.get((year, month))
            if stat:
                response[key] = {
                    "total_amount": stat.total_amount,
                    "total_count": stat.total_count,
                    "all_time_count": stat.all_time_count
                }
            else:
                response[key] = {
                    "total_amount": 0,
                    "total_count": 0,
                    "all_time_count": 0
                }

        return response
