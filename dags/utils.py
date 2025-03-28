import praw
from prawcore.exceptions import NotFound, Forbidden, TooManyRequests
import time
import random


def praw_retry(
        func,
        initial_delay: float = 1,
        exponential_base: float = 2,
        jitter: bool = True,
        max_retries: int = 4,
):
    def wrapper(*args, **kwargs):
        num_retries = 0
        delay = initial_delay

        while True:
            try:
                return func(*args, **kwargs)
            except NotFound:
                return "NotFoundError"
            except Forbidden:
                return "ForbiddenError"
            except AttributeError:
                return "AttributeError" # if on Redditor, indicates banned account
            except Exception as e:
                print(f"ERROR, RETRYING. {e}")
                num_retries += 1

                if num_retries > max_retries:
                    return "ExceededMaxRetriesError"

                delay *= exponential_base * (1 + jitter * random.random())

                time.sleep(delay)

    return wrapper


@praw_retry
def get_redditor(reddit: praw.Reddit, user_name: str) -> praw.models.Redditor:
    return reddit.redditor(user_name)

@praw_retry
def get_subreddit(reddit: praw.Reddit, subreddit_name: str) -> praw.models.Subreddit:
    return reddit.subreddit(subreddit_name)
