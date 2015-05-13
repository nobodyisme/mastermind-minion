import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from minion.config import config
from minion.logger import logger


Base = declarative_base()


def setup_session(uri):
    engine = sqlalchemy.create_engine(uri)
    try:
        engine.connect()
        logger.info('Successfully connected to db by uri {0}'.format(uri))
    except Exception as e:
        logger.exception('Failed to connect to db engine by uri {0}'.format(uri))
        raise

    return sessionmaker(bind=engine, autocommit=True)


Session = setup_session(config['db']['uri'])
