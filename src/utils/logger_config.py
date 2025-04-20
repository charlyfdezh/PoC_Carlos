"""
Utils.
"""
import logging
import dataclasses


@dataclasses.dataclass
class LoggerConfig:
    """
    Logging class for configuring logging in a PySpark application.
    """

    @staticmethod
    def configure_logger(log_file_path: str = "application.log",
                         log_level: int = logging.INFO,
                         ) -> logging.Logger:
        """
        Configure the logger to log messages to a file and to the console.
        
        :param log_file_path: Path to the log file.
        :param log_level: Logging level (default is INFO).
        """
        logger = logging.getLogger("ContosoLogger")
        logger.setLevel(log_level)

        if not logger.handlers:
            file_handler = logging.FileHandler(log_file_path)
            file_handler.setLevel(log_level)

            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            file_handler.setFormatter(formatter)

            logger.addHandler(file_handler)

        return logger
