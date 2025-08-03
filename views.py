""" Логика запросов к смартконтрактам для получения метрик """

import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from typing import Tuple, Union, List
from web3 import Web3
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, date
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError

from models import ExchangeRate, Transaction, MonthlyStats
from logger import get_logger
from database import get_db_session
from dotenv import load_dotenv


logger = get_logger()
load_dotenv()

ABI = [
    {
        "constant": True,
        "inputs": [],
        "name": "rateSTTOREtoSTT",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "rateSTTtoSTTORE",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function",
    },
]

web3 = Web3(Web3.HTTPProvider(os.environ["WEB3_PROVIDER"]))
contract = web3.eth.contract(address=Web3.to_checksum_address(os.environ["CONTRACT_ADDRESS"]), abi=ABI)

DECIMALS = 10**9


# Получает данные о курсе STT/STORE с блокчейн-контракта и возвращает нормализованные значения
async def get_exchange_rates() -> Tuple[float, float]:
    """
    Асинхронно получает значения с контракта и нормализует
    :return: (buy, sell)
    """
    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor()

    async def call_contract_method(method_name: str) -> int:
        logger.debug(f"Вызов метода контракта: {method_name}")
        try:
            value = await loop.run_in_executor(
                executor,
                lambda: getattr(contract.functions, method_name)().call()
            )
            logger.info(f"Успешный вызов {method_name}: raw value = {value}")
            return value
        except Exception as e:
            logger.error(f"Ошибка при вызове {method_name}: {repr(e)}")
            raise

    try:
        logger.debug("Начинаем получение курсов обмена STTORE ⇄ STT")

        rate_sell_raw = await call_contract_method("rateSTTOREtoSTT")
        rate_buy_raw = await call_contract_method("rateSTTtoSTTORE")

        sell = rate_sell_raw / DECIMALS
        buy = rate_buy_raw / DECIMALS

        logger.info(f"Получено: buy = {buy}, sell = {sell}")

        return buy, sell

    except Exception as e:
        logger.critical(f"Ошибка при получении курсов: {repr(e)}")
        raise


# Получает актуальные курсы и сохраняет их в таблицу ExchangeRate
async def fetch_and_store_exchange_rate() -> ExchangeRate:
    """
    Получает курсы с контракта, сохраняет их в БД
    """
    buy, sell = await get_exchange_rates()
    today = date.today()

    async with get_db_session() as session:
        try:
            rate = ExchangeRate(buy=buy, sell=sell, date=today)
            session.add(rate)
            await session.commit()
            await session.refresh(rate)
            logger.info(f"Курс успешно записан: buy={buy}, sell={sell}, date={today}")
            return rate
        except SQLAlchemyError as e:
            logger.error(f"Ошибка при записи курса в БД: {repr(e)}")
            await session.rollback()
            raise
        except Exception as e:
            logger.critical(f"Неизвестная ошибка при сохранении курса: {repr(e)}")
            await session.rollback()
            raise


# Возвращает список курсов за указанный диапазон дат
async def get_exchange_rates_range(
        start_date: Union[str, date],
        end_date: Union[str, date],
) -> List[dict]:
    """
    Получает курсы обмена из таблицы ExchangeRate в диапазоне дат.

    :param start_date: дата начала (строка 'YYYY-MM-DD' или datetime.date)
    :param end_date: дата конца (строка 'YYYY-MM-DD' или datetime.date)
    :return: список словарей с данными вида {'date': 'YYYY-MM-DD', 'buy': float, 'sell': float}
    """
    logger.debug(f"[get_exchange_rates_range] Запрос курсов с {start_date} по {end_date}")

    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    async with get_db_session() as session:
        try:
            query = (
                select(ExchangeRate)
                .where(ExchangeRate.date >= start_date)
                .where(ExchangeRate.date <= end_date)
                .order_by(ExchangeRate.date.asc())
            )
            result = await session.execute(query)
            records = result.scalars().all()
            logger.info(f"[get_exchange_rates_range] Найдено записей: {len(records)}")

            return [
                {
                    "date": record.date.isoformat(),
                    "buy": record.buy,
                    "sell": record.sell,
                }
                for record in records
            ]
        except Exception as e:
            logger.exception(f"[get_exchange_rates_range] Ошибка при выборке: {e}")
            raise


# Вычисление статистики транзакций за текущий месяц
async def calculate_monthly_stats():
    """
    Вычисляет статистику транзакций за текущий месяц и сохраняет её в таблицу MonthlyStats.

    Статистика включает:
    - year, month — текущие год и месяц
    - total_amount — общая сумма транзакций за месяц
    - total_count — количество транзакций за месяц
    - all_time_count — общее количество всех транзакций с начала времени

    :raises IntegrityError: если запись за текущий месяц уже существует
    :raises Exception: для других ошибок при работе с базой данных
    """
    today = date.today()
    year, month = today.year, today.month

    # Вычисляем границы месяца
    first_day = date(year, month, 1)
    last_day = (first_day + timedelta(days=32)).replace(day=1) - timedelta(days=1)

    logger.info(f"Начинаем сбор статистики за {month:02}.{year}")

    async with get_db_session() as session:
        try:
            # Транзакции за текущий месяц
            query = (
                select(Transaction)
                .where(Transaction.created_at >= first_day)
                .where(Transaction.created_at <= last_day)
            )
            result = await session.execute(query)
            txs = result.scalars().all()

            total_amount = sum(tx.amount for tx in txs)
            total_count = len(txs)

            logger.info(f"Транзакций за месяц: {total_count}, общая сумма: {total_amount:.2f}")

            # Общее количество всех транзакций за всю историю
            all_time_count_result = await session.execute(select(func.count(Transaction.id)))
            all_time_count = all_time_count_result.scalar()

            logger.info(f"Общее количество всех транзакций: {all_time_count}")

            # Сохраняем статистику
            stat = MonthlyStats(
                year=year,
                month=month,
                total_amount=total_amount,
                total_count=total_count,
                all_time_count=all_time_count,
            )

            session.add(stat)
            await session.commit()

            logger.info(f"Статистика успешно сохранена за {month:02}.{year}")

        except IntegrityError:
            logger.warning(f"Статистика за {month:02}.{year} уже существует в базе данных.")
            raise
        except Exception as e:
            logger.error(f"Ошибка при сохранении статистики: {e}")
            raise