from __future__ import annotations

import logging
import os


class AppLogger:
	def __init__(self, name: str = "crawler") -> None:
		self._logger = logging.getLogger(name)
		if not self._logger.handlers:
			handler = logging.StreamHandler()
			file_handler = logging.FileHandler(f"{name}.log")

			formatter = logging.Formatter(
				"%(asctime)s | %(levelname)s | %(name)s | %(message)s",
				"%Y-%m-%d %H:%M:%S",
			)

			handler.setFormatter(formatter)
			file_handler.setFormatter(formatter)
			
			self._logger.addHandler(handler)
			self._logger.addHandler(file_handler)

		level_name = os.getenv("LOG_LEVEL", "INFO").upper()
		level = getattr(logging, level_name, logging.INFO)
		self._logger.setLevel(level)
		self._logger.propagate = False

	def debug(self, message: str) -> None:
		self._logger.debug(message)

	def info(self, message: str) -> None:
		self._logger.info(message)

	def warning(self, message: str) -> None:
		self._logger.warning(message)

	def error(self, message: str) -> None:
		self._logger.error(message)

	def exception(self, message: str) -> None:
		self._logger.exception(message)
