import praw
from prawcore.exceptions import NotFound, Forbidden, TooManyRequests, Redirect
import time
import random
from sentence_transformers import SentenceTransformer
from model2vec import StaticModel
from typing import List, Dict
import itertools


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
            except Redirect:
                return "RedirectError"
            except AttributeError:
                return "AttributeError"  # if on Redditor, indicates banned account
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


class SingletonSentenceTransformer:
    """
    Allows embedding model to be created once and kept in memory.
    """
    _instance = None

    def __new__(cls, model_name='sentence-transformers/all-MiniLM-L6-v2'):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.model = SentenceTransformer(model_name)
        return cls._instance


class SingletonStaticModel:
    """
    Allows embedding model to be created once and kept in memory.
    """
    _instance = None

    def __new__(cls, model_name='minishlab/potion-base-2M'):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.model = StaticModel.from_pretrained(model_name)
        return cls._instance


class Reddit(praw.Reddit):
    def __init__(self, client_info: Dict | List[Dict]):
        self.reddit_instances = [praw.Reddit(**kwargs) for kwargs in client_info] if isinstance(client_info,
                                                                                                List) else [
            praw.Reddit(**client_info)]
        self._instance_cycle = itertools.cycle(self.reddit_instances)

    def __getattr__(self, attr):
        reddit = next(self._instance_cycle)
        return getattr(reddit, attr)
