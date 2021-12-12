import logging
import sys


class Logger(object):
    def __init__(self, cfg, class_name, log_file_with_path, is_console):
        handler = logging.FileHandler(log_file_with_path)
        formatter = logging.Formatter(cfg["output"]["log_format"])
        handler.setFormatter(formatter)
        self.logger = logging.getLogger(class_name)
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(handler)

        if is_console:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

    def get_logger(self):
        return self.logger
