from datetime import datetime, timezone, timedelta


def monitorable_post_pipeline(n_monitors, monitor_interval_hours):
    return [
        {
            "$group": {
                "_id": "$post_ref",
                "count": {"$sum": 1},
                "max_scrape_time": {"$max": "$scrape_time"}
            }
        },
        {
            "$match": {
                "count": {"$lt": n_monitors},
                "max_scrape_time": {"$lt": datetime.now(timezone.utc) - timedelta(
                    hours=monitor_interval_hours)}
            }
        },
        {
            "$lookup": {
                "from": "posts_static",  # The static collection name
                "localField": "_id",  # Field in the dynamic collection (post_ref)
                "foreignField": "_id",  # Field in the static collection (post_id)
                "as": "static_data"  # Alias for the joined data
            }
        },
        {
            "$unwind": "$static_data"
        },
        {
            "$project": {
                "_id": 0,
                "post_ref": "$_id",
                "post_id": "$static_data.post_id",
                "post_type": "$static_data.post_type"  # Include the post_type from the static collection
            }
        }
    ]


def monitorable_subreddit_pipeline(n_monitors, monitor_interval_hours):
    return [
        # Select posts from the monitoring window
        {
            "$match": {
                "post_created_utc": {"$gte": (datetime.now(timezone.utc) - timedelta(
                    hours=monitor_interval_hours * (n_monitors + 1))).timestamp()}
            }
        },
        {
            "$lookup": {
                "from": "subreddits_static",
                "localField": "post_subreddit",
                "foreignField": "subreddit_name",
                "as": "matched_subreddits"
            }
        },
        {
            "$unwind": "$matched_subreddits"
        },
        {
            "$group": {
                "_id": "$matched_subreddits._id",  # Unique subreddit IDs
                "subreddit_name": {"$first": "$matched_subreddits.subreddit_name"}
            }
        },
        {
            "$lookup": {
                "from": "subreddits_dynamic",
                "localField": "_id",
                "foreignField": "subreddit_ref",
                "as": "dynamic_subreddits"
            }
        },
        {
            "$unwind": "$dynamic_subreddits"
        },
        {
            "$group": {
                "_id": "$_id",
                "subreddit_name": {"$first": "$subreddit_name"},
                "max_scrape_time": {"$max": "$dynamic_subreddits.scrape_time"}
            }
        },
        {
            "$match": {
                "max_scrape_time": {
                    "$lte": datetime.now(timezone.utc) - timedelta(hours=monitor_interval_hours)
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "subreddit_ref": "$_id",
                "subreddit_name": 1,
                "scrape_time": "$dynamic_subreddits.scrape_time"
            }
        }
    ]


def monitorable_mods_pipeline(n_monitors, monitor_interval_hours):
    """
    TODO: should return all mods who
    - correspond to subreddits with posts within the total monitoring interval
    - have not been monitored in the current monitoring interval
    """
    return [
        # Select posts from the monitoring window
        {
            "$match": {
                "post_created_utc": {"$gte": (datetime.now(timezone.utc) - timedelta(
                    hours=monitor_interval_hours * (n_monitors + 1))).timestamp()}
            }
        },
        {
            "$lookup": {
                "from": "subreddits_static",
                "localField": "post_subreddit",
                "foreignField": "subreddit_name",
                "as": "matched_subreddits"
            }
        },
        {
            "$unwind": "$matched_subreddits"
        },
        {
            "$group": {
                "_id": "$matched_subreddits._id",  # Unique subreddit IDs
                "subreddit_name": {"$first": "$matched_subreddits.subreddit_name"}
            }
        },
        {
            "$lookup": {
                "from": "subreddits_dynamic",
                "localField": "_id",
                "foreignField": "subreddit_ref",
                "as": "dynamic_subreddits"
            }
        },
        {
            "$unwind": "$dynamic_subreddits"
        },
        {
            "$unwind": "$dynamic_subreddits.subreddit_mods_info"
        },
        {
            "$group": {
                "_id": None,
                "unique_moderators": {"$addToSet": "$dynamic_subreddits.subreddit_mods_info.user_name"}
            }
        },
        {
            "$unwind": "$unique_moderators"
        },
        {
            "$lookup": {
                "from": "moderators_static",
                "localField": "unique_moderators",
                "foreignField": "user_name",
                "as": "moderator_info"
            }
        },
        {
            "$unwind": "$moderator_info"
        },
        {
            "$lookup": {
                "from": "moderators_dynamic",
                "localField": "moderator_info._id",
                "foreignField": "mod_ref",
                "as": "dynamic_moderator_data"
            }
        },

        {
            "$group": {
                "_id": "$unique_moderators",
                "moderator_id": {"$first": "$moderator_info._id"},
                "max_scrape_time": {"$max": "$dynamic_moderator_data.scrape_time"}
            }
        },
        {
            "$match": {
                "max_scrape_time": {
                    "$lte": (datetime.now(timezone.utc) - timedelta(hours=monitor_interval_hours))  
                }
            }
        }
    ]
