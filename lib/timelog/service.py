import time
import logging
from functools import wraps

LOG_FMT = (
    "{func:<28} | "
    "{transport:<12} | "
    "{status:<10} | "
    "{elapsed:>12.6f}s | "
    "records={records}"
)

def timeit_if_returned(func):
    """Log timing only if return value is not None, including number of records and transport."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = time.perf_counter() - start

        if result is not None:
            # Extract number of records from self.max_records if possible
            records = getattr(args[0], "max_records", "N/A") if args else "N/A"
            # Derive transport from function name
            transport = func.__name__.replace("push_on_max_", "")
            
            logging.info(
                LOG_FMT.format(
                    func=func.__name__,
                    transport=transport,
                    status="returned",
                    elapsed=elapsed,
                    records=records
                )
            )
        return result
    return wrapper
