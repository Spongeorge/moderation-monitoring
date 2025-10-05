import inspect
import json
import random
import re
from datetime import datetime, timezone

import praw
from bs4 import BeautifulSoup
from prawcore.exceptions import Forbidden, NotFound, Redirect
from pymongo import MongoClient
from tqdm import tqdm

import metrics
import utils


def skip():
    return


def sample_random_subreddits(kwargs):
    client = MongoClient("mongodb://localhost:27017/")
    db = client["ContentModerationDB"]
    users_static_collection = db["users_static"]

    reddit = utils.Reddit(client_info=kwargs['config']['client_info'])

    list_of_subreddits = reddit.subreddit("ListOfSubreddits")

    def extract_subreddits(html):
        try:
            soup = BeautifulSoup(html, 'html.parser')
            subreddit_links = soup.find_all('a', href=re.compile(r'^/r/'))
            subreddits = sorted({link['href'] for link in subreddit_links if link['href'].startswith('/r/')})
            return subreddits
        except:
            return []

    # Extract all subreddits
    all_subreddits = set()
    for wiki_page in tqdm(list_of_subreddits.wiki):
        all_subreddits.update(
            [subreddit_name[3:].split('/')[0] for subreddit_name in extract_subreddits(wiki_page.content_html)])

    valid_subreddits = all_subreddits.copy()

    n_subreddits_sampled = 0

    unique_posters = set()

    users_to_ignore = {"AutoModerator"}

    while n_subreddits_sampled < kwargs['config']['num_subreddits']:
        new_sampled_subreddit = random.sample(list(valid_subreddits), 1)[0]
        valid_subreddits.remove(new_sampled_subreddit)
        try:
            unique_posters_ = set()
            unique_posters_count_ = 0
            posts_to_check = [x for x in reddit.subreddit(new_sampled_subreddit).new(limit=None)]
            while posts_to_check and unique_posters_count_ < kwargs['config']['num_users_per_subreddit']:
                submission = posts_to_check.pop()
                if (submission.author) and (submission.author.name not in unique_posters):
                    unique_posters_.add(submission.author.name)
                    unique_posters_count_ += 1

            for unique_poster in (unique_posters_ - unique_posters) - users_to_ignore:
                redditor = reddit.redditor(unique_poster)

                if hasattr(redditor, 'created_utc'):  # if account doesn't have this it is shadowbanned
                    user_update = {metric_name: getattr(metrics, metric_name)(reddit.redditor(unique_poster)) for
                                   metric_name in
                                   kwargs['config']['user_metrics_static']}  # get all configured metric values
                    users_static_collection.insert_one(user_update)

            unique_posters.update(unique_posters_)
            n_subreddits_sampled += 1
            print(n_subreddits_sampled)

        except (NotFound, Forbidden, Redirect):  # Subreddit is banned or private, re-sample
            print("Bad subreddit... retrying.")


def populate_from_list(kwargs):
    client = MongoClient("mongodb://localhost:27017/")
    db = client["ContentModerationDB"]

    reddit = utils.Reddit(client_info=kwargs['config']['client_info'])

    assert 'subreddit_list_path' in kwargs[
        'config'].keys(), "Tried to populate from list, but subreddit_list_path missing from config."

    with open(kwargs['config']['subreddit_list_path'], 'r') as f:
        subreddit_names = json.load(f)

    for subreddit_name in subreddit_names:

        subreddit = reddit.subreddit(subreddit_name)

        if db['subreddits_static'].find_one({"subreddit_name": subreddit_name}) is None:
            subreddit_static_entry = {metric_name: getattr(metrics, metric_name)(subreddit) for
                                      metric_name in
                                      kwargs['config']['subreddit_metrics_static']}

            static_result = db["subreddits_static"].insert_one(
                subreddit_static_entry
            )

            subreddit_dynamic_entry = {metric_name: getattr(metrics, metric_name)(subreddit) for
                                       metric_name in
                                       kwargs['config']['subreddit_metrics_dynamic']}

            subreddit_dynamic_entry["scrape_time"] = datetime.now(timezone.utc)
            subreddit_dynamic_entry["subreddit_ref"] = static_result.inserted_id

            db["subreddits_dynamic"].insert_one(
                subreddit_dynamic_entry
            )

            if "subreddit_mods_info" in subreddit_dynamic_entry not isinstance(subreddit_dynamic_entry["subreddit_mods_info"], str):
                for mod in subreddit_dynamic_entry['subreddit_mods_info']:
                    if db['moderators_static'].find_one(
                            {"user_name": mod['user_name']}) is None:  # not already in DB

                        mod = utils.get_redditor(reddit, mod['user_name'])

                        if isinstance(mod,
                                      praw.models.Redditor):  # Ensures a valid redditor is returned

                            mod_static_entry = {metric_name: getattr(metrics, metric_name)(mod) for
                                                metric_name in
                                                kwargs['config']['moderator_metrics_static']}

                            static_result = db["moderators_static"].insert_one(
                                mod_static_entry
                            )

                            mod_static_id = static_result.inserted_id

                            mod_dynamic_entry = {
                                metric_name: (
                                    getattr(metrics, metric_name)(mod, return_default=True)
                                    if "return_default" in inspect.signature(
                                        getattr(metrics, metric_name)).parameters
                                    else getattr(metrics, metric_name)(mod)
                                )
                                for metric_name in kwargs['config']['moderator_metrics_dynamic']
                            }

                            mod_dynamic_entry["scrape_time"] = datetime.now(timezone.utc)
                            mod_dynamic_entry["mod_ref"] = mod_static_id

                            db["moderators_dynamic"].insert_one(
                                mod_dynamic_entry
                            )

