#
#  Copyright 2019 The Eggroll Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import inspect
import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler
from threading import RLock

from eggroll.utils import file_utils

logging.TRACE = logging.DEBUG - 5
logging.addLevelName(logging.DEBUG - 5, "TRACE")
def trace(self, msg, *args, **kwargs):
    if self.isEnabledFor(logging.TRACE):
        self._log(logging.TRACE, msg, args, **kwargs)
logging.Logger.trace = trace


class LoggerFactory(object):
    TYPE = "FILE"
    LEVEL = os.environ.get('EGGROLL_LOG_LEVEL', logging.INFO)
    name_to_loggers = {}
    LOG_DIR = None
    lock = RLock()
    default_logger_name = os.environ.get('EGGROLL_LOG_FILE', default=f'eggroll')
    default_log_formatter = logging.Formatter('[%(levelname)-5s][%(asctime)s][%(threadName)s,pid:%(process)d,tid:%(thread)d][%(filename)s:%(lineno)s.%(funcName)s] - %(message)s')


    @staticmethod
    def set_directory(directory=None):
        with LoggerFactory.lock:
            if not directory:
                directory = os.path.join(file_utils.get_project_base_directory(),
                                         'logs')
            LoggerFactory.LOG_DIR = directory
            os.makedirs(LoggerFactory.LOG_DIR, exist_ok=True)
            for name, (logger, handler) in LoggerFactory.name_to_loggers.items():
                logger.removeHandler(handler)
                handler.close()
                _handler = LoggerFactory.get_handler(name)
                logger.addHandler(_handler)
                LoggerFactory.name_to_loggers[name] = logger, _handler

    @staticmethod
    def get_logger(name):
        with LoggerFactory.lock:
            if not LoggerFactory.LOG_DIR:
                LoggerFactory.LOG_DIR = os.environ.get('EGGROLL_LOGS_DIR', default=f'{os.environ["EGGROLL_HOME"]}/logs/eggroll')
                os.makedirs(LoggerFactory.LOG_DIR, exist_ok=True)
            if name in LoggerFactory.name_to_loggers.keys():
                logger, handler = LoggerFactory.name_to_loggers[name]
                if not logger:
                    logger, handler = LoggerFactory.__init_logger(name)
            else:
                logger, handler = LoggerFactory.__init_logger(name)
            return logger

    @staticmethod
    def get_handler(name):
        if not LoggerFactory.LOG_DIR:
            return logging.StreamHandler(sys.stdout)
        formatter = LoggerFactory.default_log_formatter
        log_file = os.path.join(LoggerFactory.LOG_DIR, f"{name}.py.log")
        handler = TimedRotatingFileHandler(log_file,
                                           when='H',
                                           interval=1,
                                           backupCount=0,
                                           delay=True)

        handler.setFormatter(formatter)
        return handler

    @staticmethod
    def __init_logger(name):
        with LoggerFactory.lock:
            logger = logging.getLogger(name)
            logger.setLevel(LoggerFactory.LEVEL)
            handler = LoggerFactory.get_handler(name)
            logger.addHandler(handler)

            # also log to console if log level <= debug
            if logging._checkLevel(LoggerFactory.LEVEL) <= logging.DEBUG or os.environ.get("EGGROLL_LOG_CONSOLE") == "1":
                console_handler = logging.StreamHandler(sys.stdout)
                console_handler.setLevel(LoggerFactory.LEVEL)
                formatter = LoggerFactory.default_log_formatter
                console_handler.setFormatter(formatter)
                logger.addHandler(console_handler)

            # log error to a separate file
            error_log_file = os.path.join(LoggerFactory.LOG_DIR, f"{name}.py.err.log")
            error_handler = TimedRotatingFileHandler(error_log_file,
                                                     when='H',
                                                     interval=1,
                                                     backupCount=0,
                                                     delay=True)
            error_handler.setLevel(logging.ERROR)
            error_handler.setFormatter(LoggerFactory.default_log_formatter)
            logger.addHandler(error_handler)

            LoggerFactory.name_to_loggers[name] = logger, handler
            return logger, handler


def set_directory(directory=None):
    LoggerFactory.set_directory(directory)


def set_level(level):
    LoggerFactory.LEVEL = level


def get_logger(name=None, use_class_name=False, filename=None):
    if not name:
        if not use_class_name:
            if not filename:
                logger_name = LoggerFactory.default_logger_name
            else:
                logger_name = filename
        else:
            frame = inspect.stack()[1]
            module = inspect.getmodule(frame[0])
            logger_name = os.path.splitext(os.path.basename(module.__file__))[0]
    else:
        logger_name = name

    return LoggerFactory.get_logger(logger_name)


def set_default_logger_name(name):
    if not name:
        raise ValueError(f'setting Illegal logger name: {name}')
    LoggerFactory.default_logger_name = name