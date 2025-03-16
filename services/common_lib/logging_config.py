import logging
from pythonjsonlogger import jsonlogger

logger = logging.getLogger("householdiq_aggregator")
logger.setLevel(logging.DEBUG)

logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter(fmt="%(asctime)s %(name)s %(levelname)s %(message)s")
logHandler.setFormatter(formatter)

logger.addHandler(logHandler)
