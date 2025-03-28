import ast

import inspect
import itertools

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, timezone
import praw
from bson import ObjectId
from prawcore.exceptions import NotFound, Forbidden
from bs4 import BeautifulSoup
import re
import random
from tqdm import tqdm
from collections import defaultdict
import json

from pymongo import MongoClient

import metrics

import time

from pathlib import Path

import utils
from utils import praw_retry
import pipelines

DATA_ROOT = Path(__file__).resolve().parent.parent

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}


def sample_reddit_users(**kwargs):
    num_subreddits = kwargs.get('num_subreddits')
    num_users_per_subreddit = kwargs.get('num_users_per_subreddit')
    skip_sample = kwargs.get('skip_sample')

    client = MongoClient("mongodb://localhost:27017/")
    db = client["ContentModerationDB"]
    users_static_collection = db["users_static"]

    if skip_sample:
        return

    with open(DATA_ROOT / 'config.json', 'r') as f:
        config = json.load(f)

    reddit = praw.Reddit(
        client_id=kwargs['REDDIT_CLIENT_ID'],
        client_secret=kwargs['REDDIT_SECRET'],
        password=kwargs['REDDIT_PW'],
        user_agent="testscript",
        username=kwargs['REDDIT_USER'],
        check_for_async=False
    )

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

    while n_subreddits_sampled < num_subreddits:
        new_sampled_subreddit = random.sample(list(valid_subreddits), 1)[0]
        valid_subreddits.remove(new_sampled_subreddit)
        try:
            unique_posters_ = set()
            unique_posters_count_ = 0
            posts_to_check = [x for x in reddit.subreddit(new_sampled_subreddit).new(limit=None)]
            while posts_to_check and unique_posters_count_ < num_users_per_subreddit:
                submission = posts_to_check.pop()
                if (submission.author) and (submission.author.name not in unique_posters):
                    unique_posters_.add(submission.author.name)
                    unique_posters_count_ += 1

            for unique_poster in unique_posters_ - unique_posters:
                redditor = reddit.redditor(unique_poster)

                if hasattr(redditor, 'created_utc'):  # if account doesn't have this it is shadowbanned
                    user_update = {metric_name: getattr(metrics, metric_name)(reddit.redditor(unique_poster)) for
                                   metric_name in
                                   config['user_metrics_static']}  # get all configured metric values
                    users_static_collection.insert_one(user_update)

            unique_posters.update(unique_posters_)
            n_subreddits_sampled += 1
            print(n_subreddits_sampled)

        except:  # Subreddit is banned or private, re-sample
            print("Bad subreddit... retrying.")

    users_to_ignore = {"AutoModerator"}
    unique_posters = unique_posters - users_to_ignore

    reddit = praw.Reddit(
        client_id=kwargs['REDDIT_CLIENT_ID'],
        client_secret=kwargs['REDDIT_SECRET'],
        password=kwargs['REDDIT_PW'],
        user_agent="testscript",
        username=kwargs['REDDIT_USER'],
        check_for_async=False
    )


def stream_reddit_posts(**kwargs):
    client = MongoClient("mongodb://localhost:27017/")

    db = client["ContentModerationDB"]

    unique_posters = db["users_static"].distinct("user_name")

    ti = kwargs['ti']

    kwargs['ti'].xcom_push(key='streaming_has_finished', value=False)

    start_time = datetime.now(timezone.utc)
    duration = timedelta(days=kwargs['stream_duration_days'])

    if not unique_posters:
        raise ValueError("No users were sampled from the previous task.")

    reddit = praw.Reddit(
        client_id=kwargs['REDDIT_CLIENT_ID'],
        client_secret=kwargs['REDDIT_SECRET'],
        password=kwargs['REDDIT_PW'],
        user_agent="testscript",
        username=kwargs['REDDIT_USER'],
        check_for_async=False
    )

    redditor_streams = {
        poster: {"comments": reddit.redditor(poster).stream.comments(pause_after=-1, skip_existing=True),
                 "submissions": reddit.redditor(poster).stream.submissions(pause_after=-1, skip_existing=True)} for
        poster in unique_posters}

    print(f"Initialized {len(redditor_streams)} redditor streams.")

    def get_comments(stream):
        comment_list = []
        try:
            for comment in stream['comments']:
                if comment is not None:
                    comment_list.append(comment)
                else:
                    return comment_list
            return comment_list
        except:  # User is suspended
            return comment_list

    def get_submissions(stream):
        submission_list = []
        try:
            for submission in stream['submissions']:
                if submission is not None:
                    submission_list.append(submission)
                else:
                    return submission_list
            return submission_list
        except:  # User is suspended
            return submission_list

    while datetime.now(timezone.utc) - start_time < duration:
        post_count = 0

        for poster in tqdm(unique_posters):
            try:
                stream = redditor_streams[poster]

                comments = [comment for comment in get_comments(stream) if
                            comment is not None]
                submissions = [submission for submission in get_submissions(stream) if
                               submission is not None]

                post_count += len(comments) + len(submissions)
                for post in itertools.chain(comments, submissions):

                    created_utc = metrics.post_created_utc(post)

                    if datetime.now(timezone.utc) - datetime.utcfromtimestamp(created_utc).replace(
                            tzinfo=timezone.utc) <= timedelta(
                        hours=kwargs['monitor_interval_hours']):  # post have been made within the monitoring interval

                        post_static_entry = {metric_name: getattr(metrics, metric_name)(post) for
                                             metric_name in
                                             config['post_metrics_static']}

                        static_result = db["posts_static"].insert_one(
                            post_static_entry
                        )

                        post_static_id = static_result.inserted_id

                        post_dynamic_entry = {
                            metric_name: (
                                getattr(metrics, metric_name)(post, return_default=True)
                                if "return_default" in inspect.signature(getattr(metrics, metric_name)).parameters
                                else getattr(metrics, metric_name)(post)
                            )
                            for metric_name in config['post_metrics_dynamic']
                        }

                        post_dynamic_entry["scrape_time"] = datetime.now(timezone.utc)
                        post_dynamic_entry["post_ref"] = post_static_id

                        db["posts_dynamic"].insert_one(
                            post_dynamic_entry
                        )

                        # If subreddit not already in database, add it
                        if db['subreddits_static'].find_one({"subreddit_name": post.subreddit.display_name}) is None:
                            subreddit_static_entry = {metric_name: getattr(metrics, metric_name)(post.subreddit) for
                                                      metric_name in
                                                      config['subreddit_metrics_static']}

                            static_result = db["subreddits_static"].insert_one(
                                subreddit_static_entry
                            )

                            subreddit_dynamic_entry = {metric_name: getattr(metrics, metric_name)(post.subreddit) for
                                                       metric_name in
                                                       config['subreddit_metrics_dynamic']}

                            subreddit_dynamic_entry["scrape_time"] = datetime.now(timezone.utc)
                            subreddit_dynamic_entry["subreddit_ref"] = static_result.inserted_id

                            db["subreddits_dynamic"].insert_one(
                                subreddit_dynamic_entry
                            )

                            if "subreddit_mods_info" in subreddit_dynamic_entry:
                                for mod in subreddit_dynamic_entry['subreddit_mods_info']:
                                    if db['moderators_static'].find_one(
                                            {"user_name": mod['user_name']}) is None:  # not already in DB

                                        mod = utils.get_redditor(reddit, mod['user_name'])

                                        if isinstance(mod,
                                                      praw.models.Redditor):  # Ensures a valid redditor is returned

                                            mod_static_entry = {metric_name: getattr(metrics, metric_name)(mod) for
                                                                metric_name in
                                                                config['mod_metrics_static']}

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
                                                for metric_name in config['mod_metrics_dynamic']
                                            }

                                            mod_dynamic_entry["scrape_time"] = datetime.now(timezone.utc)
                                            mod_dynamic_entry["mod_ref"] = mod_static_id

                                            db["moderators_dynamic"].insert_one(
                                                mod_dynamic_entry
                                            )

            except (NotFound, Forbidden):  # User's account was suspended
                unique_posters.remove(poster)


@praw_retry
def get_comment(reddit, id):
    return reddit.comment(id=id)


@praw_retry
def get_submission(reddit, id):
    return reddit.submission(id=id)


def monitor(**kwargs):
    ti = kwargs['ti']

    streaming_finished = ti.xcom_pull(task_ids='stream', key='streaming_has_finished')
    monitoring_finished = False

    client = MongoClient("mongodb://localhost:27017/")

    db = client["ContentModerationDB"]

    while (not streaming_finished) or (not monitoring_finished):
        reddit = praw.Reddit(
            client_id=kwargs['REDDIT_CLIENT_ID'],
            client_secret=kwargs['REDDIT_SECRET'],
            password=kwargs['REDDIT_PW'],
            user_agent="testscript",
            username=kwargs['REDDIT_USER'],
            check_for_async=False
        )

        # Post Monitoring
        monitorable_posts = list(db['posts_dynamic'].aggregate(
            pipelines.monitorable_post_pipeline(kwargs['n_monitors'], kwargs['monitor_interval_hours'])))

        for monitorable_post in monitorable_posts:
            if monitorable_post['post_type'] == 'comment':
                post = get_comment(reddit, monitorable_post['post_id'])
            elif monitorable_post['post_type'] == 'submission':
                post = get_submission(reddit, monitorable_post['post_id'])

            post_dynamic_entry = {
                metric_name: (
                    getattr(metrics, metric_name)(post, return_default=True)
                    if "return_default" in inspect.signature(getattr(metrics, metric_name)).parameters
                    else getattr(metrics, metric_name)(post)
                )
                for metric_name in config['post_metrics_dynamic']
            }
            post_dynamic_entry["scrape_time"] = datetime.now(timezone.utc)
            post_dynamic_entry["post_ref"] = monitorable_post['post_ref']

            db["posts_dynamic"].insert_one(
                post_dynamic_entry
            )

            # Subreddit Monitoring
            monitorable_subreddits = list(db['posts_static'].aggregate(
                pipelines.monitorable_subreddit_pipeline(kwargs['n_monitors'], kwargs['monitor_interval_hours'])))

            if len(monitorable_subreddits) > 0:
                print(f"Found {len(monitorable_subreddits)} monitorable subreddits.")

            for monitorable_subreddit in monitorable_subreddits:
                sr = utils.get_subreddit(reddit, monitorable_subreddit['subreddit_name'])

                subreddit_dynamic_entry = {
                    metric_name: (
                        getattr(metrics, metric_name)(sr, return_default=True)
                        if "return_default" in inspect.signature(getattr(metrics, metric_name)).parameters
                        else getattr(metrics, metric_name)(sr)
                    )
                    for metric_name in config['subreddit_metrics_dynamic']
                }
                subreddit_dynamic_entry["scrape_time"] = datetime.now(timezone.utc)
                subreddit_dynamic_entry["subreddit_ref"] = monitorable_subreddit['subreddit_ref']

                db["subreddits_dynamic"].insert_one(
                    subreddit_dynamic_entry
                )

                # Moderator Monitoring
                monitorable_mods = list(db['posts_static'].aggregate(
                    pipelines.monitorable_mods_pipeline(kwargs['n_monitors'], kwargs['monitor_interval_hours'])))

                if len(monitorable_mods) > 0:
                    print(f"Found {len(monitorable_mods)} monitorable moderators.")

                for monitorable_mod in monitorable_mods:
                    mod = utils.get_redditor(reddit, monitorable_mod['_id'])

                    moderator_dynamic_entry = {
                        metric_name: (
                            getattr(metrics, metric_name)(mod)
                            if "return_default" in inspect.signature(getattr(metrics, metric_name)).parameters
                            else getattr(metrics, metric_name)(mod)
                        )
                        for metric_name in config['moderator_metrics_dynamic']
                    }
                    moderator_dynamic_entry["scrape_time"] = datetime.now(timezone.utc)
                    moderator_dynamic_entry["mod_ref"] = monitorable_mod['moderator_id']

                    db["moderators_dynamic"].insert_one(
                        moderator_dynamic_entry
                    )
        # TODO:
        # # Mod Monitoring
        # monitorable_mods = list(db['moderators_dynamic'].aggregate(
        #     monitorable_post_pipeline(kwargs['n_monitors'], kwargs['monitor_interval_hours'])))
        #
        # for monitorable_post in monitorable_posts:
        #     if monitorable_post['post_type'] == 'comment':
        #         post = get_comment(reddit, monitorable_post['post_id'])
        #     elif monitorable_post['post_type'] == 'submission':
        #         post = get_submission(reddit, monitorable_post['post_id'])
        #
        #     post_dynamic_entry = {
        #         metric_name: (
        #             getattr(metrics, metric_name)(post, return_default=True)
        #             if "return_default" in inspect.signature(getattr(metrics, metric_name)).parameters
        #             else getattr(metrics, metric_name)(post)
        #         )
        #         for metric_name in config['post_metrics_dynamic']
        #     }
        #     post_dynamic_entry["scrape_time"] = datetime.now(timezone.utc)
        #     post_dynamic_entry["post_ref"] = monitorable_post['post_ref']
        #
        #     db["posts_dynamic"].insert_one(
        #         post_dynamic_entry
        #     )

        if len(monitorable_posts) == 0:
            monitoring_finished = True
        else:
            monitoring_finished = False


def monitor_moderation_teams():
    pass


with DAG(
        'content_moderation_data_collection',
        default_args=default_args,
        description='Sample usernames, stream posts, and monitor post variables',
        schedule_interval=None,
        catchup=False,
) as dag:
    with open(DATA_ROOT / 'config.json', 'r') as f:
        config = json.load(f)

    print(DATA_ROOT / 'config.json')
    print(config)
    sample_task = PythonOperator(
        task_id='sample',
        python_callable=sample_reddit_users,
        provide_context=True,
        op_kwargs={
            'num_subreddits': config['n_subreddits'],
            'num_users_per_subreddit': config['users_per_subreddit'],
            'REDDIT_USER': config['REDDIT_USER_1'],
            'REDDIT_CLIENT_ID': config['REDDIT_CLIENT_ID_1'],
            'REDDIT_SECRET': config['REDDIT_SECRET_1'],
            'REDDIT_PW': config['REDDIT_PW_1'],
            'skip_sample': config['skip_sample']
        },
    )

    stream_task = PythonOperator(
        task_id='stream',
        python_callable=stream_reddit_posts,
        provide_context=True,
        op_kwargs={
            'REDDIT_USER': config['REDDIT_USER_1'],
            'REDDIT_CLIENT_ID': config['REDDIT_CLIENT_ID_1'],
            'REDDIT_SECRET': config['REDDIT_SECRET_1'],
            'REDDIT_PW': config['REDDIT_PW_1'],
            'start_time': '{{ ts }}',
            'stream_duration_days': config['stream_duration_days'],
            'monitor_interval_hours': config['monitor_interval_hours']
        },
    )

    monitor_task = PythonOperator(
        task_id='monitor',
        python_callable=monitor,
        provide_context=True,
        op_kwargs={
            'monitor_interval_hours': config['monitor_interval_hours'],
            'n_monitors': config['n_monitors'],
            'REDDIT_USER': config['REDDIT_USER_2'],
            'REDDIT_CLIENT_ID': config['REDDIT_CLIENT_ID_2'],
            'REDDIT_SECRET': config['REDDIT_SECRET_2'],
            'REDDIT_PW': config['REDDIT_PW_2']
        },
    )

    sample_task >> [stream_task, monitor_task]
