import time
import random
import functools
import logging

logger = logging.getLogger(__name__)


def retry(
    max_attempts=3,
    backoff_factor=1.0,
    retry_on=(Exception,),
    jitter=0.1,
):

    def decorator(fn):

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):

            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except retry_on as e:
                    if attempt == max_attempts:
                        logger.error("Retry failed after %s attempts", max_attempts)
                        raise

                    sleep_time = backoff_factor * (2 ** (attempt - 1))
                    sleep_time += random.uniform(0, jitter * sleep_time)

                    logger.warning(
                        "[%s] attempt %s/%s failed: %s -> retry in %.2fs",
                        fn.__name__, attempt, max_attempts, e, sleep_time
                    )

                    time.sleep(sleep_time)

        return wrapper

    return decorator
