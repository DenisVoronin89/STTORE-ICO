import asyncio
import os
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from starlette.status import HTTP_403_FORBIDDEN
from typing import List, Union
from datetime import date
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import select, tuple_
from collections import OrderedDict

from logger import get_logger
from dotenv import load_dotenv
from database import init_db, get_db_session
from views import fetch_and_store_exchange_rate, calculate_monthly_stats, get_exchange_rates, get_exchange_rates_range
from models import MonthlyStats, ExchangeRate, MonthlyStats

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

# Настройка CORS (Это Максу - разрешить доступ фронту, разрешить отправлять мне запросы)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Разрешенные источники
    allow_credentials=True,
    allow_methods=["*"],  # Разрешенные методы
    allow_headers=["*"],  # Разрешенные заголовки
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
@app.get("/api/exchange_rates")
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
@app.post("/api/transaction")
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
@app.get("/api/monthly_stats")
async def get_monthly_stats(
    from_year: int = Query(..., alias="from_year"),
    from_month: int = Query(..., alias="from_month"),
    to_year: int = Query(..., alias="to_year"),
    to_month: int = Query(..., alias="to_month"),
):
    logger.debug(f"Получен запрос /monthly_stats с параметрами: "
                 f"from_year={from_year}, from_month={from_month}, to_year={to_year}, to_month={to_month}")

    if (to_year, to_month) < (from_year, from_month):
        logger.error("Некорректный диапазон дат: конец раньше начала")
        raise HTTPException(status_code=400, detail="Некорректный диапазон")

    # Сформировать список месяцев
    months = []
    year, month = from_year, from_month
    while (year, month) <= (to_year, to_month):
        months.append((year, month))
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
    logger.debug(f"Сформирован список месяцев для выборки: {months}")

    async with get_db_session() as session:
        logger.debug("Открыта сессия с базой данных")
        try:
            result = await session.execute(
                select(MonthlyStats).where(
                    tuple_(MonthlyStats.year, MonthlyStats.month).in_(months)
                )
            )
            stats = result.scalars().all()
            logger.debug(f"Получено из базы {len(stats)} записей статистики")
        except Exception as e:
            logger.exception(f"Ошибка при выполнении запроса к базе: {e}")
            raise HTTPException(status_code=500, detail="Ошибка доступа к базе данных")

        # Создаем мапу для удобного поиска
        stats_map = {(s.year, s.month): s for s in stats}
        logger.debug(f"Создана карта статистики по годам и месяцам")

        response = OrderedDict()
        for year, month in months:
            key = f"{year:04d}-{month:02d}"
            stat = stats_map.get((year, month))
            if stat:
                logger.debug(f"Добавляем данные за {key}: amount={stat.total_amount}, count={stat.total_count}, all_time_count={stat.all_time_count}")
                response[key] = {
                    "total_amount": stat.total_amount,
                    "total_count": stat.total_count,
                    "all_time_count": stat.all_time_count
                }
            else:
                logger.debug(f"Данных за {key} нет, заполняем нулями")
                response[key] = {
                    "total_amount": 0,
                    "total_count": 0,
                    "all_time_count": 0
                }

    logger.info(f"Отправляем ответ с {len(response)} месяцами статистики")
    return response

