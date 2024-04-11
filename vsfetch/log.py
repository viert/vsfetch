import sys
import logging

log = logging.getLogger(__name__)
log.propagate = False
log.setLevel(logging.DEBUG)

for handler in log.handlers:
    log.removeHandler(handler)

formatter = logging.Formatter("[%(asctime)s] %(levelname)s %(message)s")
handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(formatter)

log.addHandler(handler)
