import logging
import sys

class Logger:
    """
    This is a simple Yarn Logger class.
    """
    def get_logger(name: str, level=logging.INFO) -> logging.Logger:
        """
        Creates a YARN loggers instance. The default logger level is INO.
        :param level:
        :return:
        """
        logger = logging.getLogger(name)
        logger.setLevel(level)
        if logger.handlers:
            pass
        else:
            ch = logging.StreamHandler(sys.stderr)
            ch.setLevel(level)
            formatter = logging.Formatter(
                '%(asctime)s.%(msecs)03d %(levelname)s %(pathname)s %(module)s - %(funcName)s: %(message)s')
            ch.setFormatter(formatter)
            logger.addHandler(ch)
        return logger
