""" Логика запросов к смартконтрактам для получения метрик """

import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from typing import Tuple, Union, List
from web3 import Web3
from sqlalchemy import select, text
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
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
    {
        "constant": True,
        "inputs": [{"name": "decimals", "type": "uint8"}],
        "name": "getTotalDistributedInTokens",
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


# Получение статистики распределенных токенов за текущий месяц
async def calculate_monthly_stats():
    """
    Получает общую сумму распределенных токенов STT с контракта и сохраняет её в таблицу MonthlyStats.

    Статистика включает:
    - year, month — текущие год и месяц
    - total_amount — общая сумма распределенных токенов STT (с учетом decimals)

    :raises IntegrityError: если запись за текущий месяц уже существует
    :raises Exception: для других ошибок при работе с базой данных или контрактом
    """
    today = date.today()
    year, month = today.year, today.month

    # Вычисляем границы месяца
    first_day = date(year, month, 1)
    last_day = (first_day + timedelta(days=32)).replace(day=1) - timedelta(days=1)

    logger.info(f"Начинаем сбор статистики за {month:02}.{year}")

    STT_SERVICE_DISTRIBUTOR_ADDRESS = "0xa6aD0a03D7873A9b9CF49648ae2563Ca7F031f6a"
    distributor_contract = web3.eth.contract(address=Web3.to_checksum_address(STT_SERVICE_DISTRIBUTOR_ADDRESS), abi=ABI)

    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor()

    try:
        logger.debug("Вызов метода контракта: getTotalDistributedInTokens(9)")
        total_amount_raw = await loop.run_in_executor(
            executor,
            lambda: distributor_contract.functions.getTotalDistributedInTokens(9).call()
        )
        
        logger.info(f"Получено из контракта: total_amount_raw (tokens) = {total_amount_raw}")
        
        total_amount = round(total_amount_raw / 1300000, 3)
        logger.info(f"После деления на 1300000: total_amount = {total_amount}")

        # Сохраняем статистику в БД
        async with get_db_session() as session:
            try:
                await session.execute(text(
                    "SELECT setval('monthly_stats_id_seq', COALESCE((SELECT MAX(id) FROM monthly_stats), 0) + 1, false)"
                ))
                stat = MonthlyStats(
                    year=year,
                    month=month,
                    total_amount=total_amount,
                )

                session.add(stat)
                await session.commit()

                logger.info(f"Статистика успешно сохранена за {month:02}.{year}: total_amount = {total_amount}")

            except IntegrityError:
                logger.warning(f"Статистика за {month:02}.{year} уже существует в базе данных.")
                raise
            except Exception as e:
                logger.error(f"Ошибка при сохранении статистики: {e}")
                raise

    except Exception as e:
        logger.error(f"Ошибка при вызове метода контракта getTotalDistributedInTokens: {repr(e)}")
        raise