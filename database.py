""" Модуль для настройки и управления взаимодействием с БД  """

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from contextlib import asynccontextmanager
from typing import AsyncGenerator
import os
from dotenv import load_dotenv

from models import Base

from logger import get_logger

logger = get_logger()

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")


# Конфиги БД
engine = create_async_engine(DATABASE_URL, echo=True)  # echo=True(для логов)
SessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)


async def init_db():
    """Функция инициализации БД, создание таблиц"""
    try:
        logger.debug("Начало инициализации базы данных...")
        async with engine.begin() as conn:
            # Создание всех таблиц, связанных с моделями, если они еще не созданы
            await conn.run_sync(Base.metadata.create_all)
        logger.info("База данных успешно инициализирована.")
    except SQLAlchemyError as e:
        logger.error(f"Ошибка при инициализации базы данных: {repr(e)}.")
        raise
    except Exception as e:
        logger.critical(f"Неожиданная ошибка при инициализации БД: {repr(e)}.")
        raise


# Асинхронный контекстный менеджер для воркера
@asynccontextmanager
async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """Асинхронный контекстный менеджер для получения сессии работы с БД"""
    logger.info("Открытие сессии с БД...")
    async with SessionLocal() as session:
        try:
            logger.info("Сессия с БД успешно открыта.")
            yield session
        except Exception as e:
            logger.error(f"Ошибка в контексте сессии: {e}")
            raise
        finally:
            logger.info("Закрытие сессии с БД.")