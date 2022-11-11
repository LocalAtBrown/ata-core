import logging.config

# TODO: As soon as we have a top-level place to store all config variables, move this into it
# Details here: https://docs.python.org/3.9/library/logging.config.html#logging.config.dictConfig
CONFIG = {
    "version": 1,
    "disable_existing_loggers": True,  # Disables some annoying logs from boto3
    "formatters": {
        "standard": {
            "format": "%(asctime)s.%(msecs).3d  %(levelname)-8s  %(name)s: %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "handlers": {
        "default": {"formatter": "standard", "class": "logging.StreamHandler"},
    },
    "root": {"handlers": ["default"], "level": "INFO"},
}

logging.config.dictConfig(CONFIG)

# import logging.config also brings logging to the global namepsace.
# Therefore, to use this logging config in another python module, we can do:
# >>> from src.helpers.logging import logging
# >>> logger = logging.getLogger(__name__)
# >>> logger.info("Hello, I'm a log")
