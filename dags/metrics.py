import praw
from utils import praw_retry, SingletonSentenceTransformer, SingletonStaticModel
from sentence_transformers import SentenceTransformer
import numpy as np


@praw_retry
def post_type(post: praw.models.Comment | praw.models.Submission) -> str:
    if isinstance(post, praw.models.Comment):
        return 'comment'
    elif isinstance(post, praw.models.Submission):
        return 'submission'


@praw_retry
def post_author(post: praw.models.Comment | praw.models.Submission) -> str:
    return post.author.name


@praw_retry
def post_body(post: praw.models.Comment | praw.models.Submission) -> str:
    if isinstance(post, praw.models.Comment):
        return post.body
    elif isinstance(post, praw.models.Submission):
        return post.selftext


@praw_retry
def post_id(post: praw.models.Comment | praw.models.Submission) -> str:
    return post.id


@praw_retry
def post_title(post: praw.models.Comment | praw.models.Submission) -> str:
    if isinstance(post, praw.models.Comment):
        return ''
    elif isinstance(post, praw.models.Submission):
        return post.title


@praw_retry
def post_created_utc(post: praw.models.Comment | praw.models.Submission) -> int:
    return post.created_utc


@praw_retry
def post_subreddit(post: praw.models.Comment | praw.models.Submission) -> str:
    return post.subreddit.display_name


@praw_retry
def post_subreddit_subscribers(post: praw.models.Comment | praw.models.Submission) -> int:
    return post.subreddit.subscribers


@praw_retry
def post_top_level_replies(post: praw.models.Comment | praw.models.Submission, return_default=False) -> int:
    if return_default:
        return 0
    if isinstance(post, praw.models.Comment):
        return post.replies.__len__()
    elif isinstance(post, praw.models.Submission):
        return post.comments.__len__()


# Doesn't recursively load CommentForests to save API usage
@praw_retry
def post_total_replies(post: praw.models.Comment | praw.models.Submission, return_default=False) -> int:
    if return_default:
        return 0
    if isinstance(post, praw.models.Comment):
        return len(post.replies.list())
    elif isinstance(post, praw.models.Submission):
        return len(post.comments.list())


@praw_retry
def post_is_removed(post: praw.models.Comment | praw.models.Submission, return_default=False) -> bool:
    if return_default:
        return False
    if isinstance(post, praw.models.Comment):
        return (post.body == '[removed]') and (post.author is None)
    elif isinstance(post, praw.models.Submission):
        return (post.selftext == '[removed]') and (post.author is None)


@praw_retry
def post_is_deleted(post: praw.models.Comment | praw.models.Submission, return_default=False) -> bool:
    if return_default:
        return False
    if isinstance(post, praw.models.Comment):
        return (post.body == '[deleted]') and (post.author is None)
    elif isinstance(post, praw.models.Submission):
        return (post.selftext == '[deleted]') and (post.author is None)


@praw_retry
def post_score(post: praw.models.Comment | praw.models.Submission, return_default=False) -> int:
    if return_default:
        return 1
    return post.score


@praw_retry
def subreddit_name(subreddit: praw.models.Subreddit) -> str:
    return subreddit.display_name


@praw_retry
def subreddit_creation_date(subreddit: praw.models.Subreddit) -> int:
    return subreddit.created_utc

@praw_retry
def subreddit_is18plus(subreddit: praw.models.Subreddit) -> bool:
    return subreddit.over18


@praw_retry
def subreddit_n_rules(subreddit: praw.models.Subreddit) -> int:
    return len(subreddit.rules()['rules'])

@praw_retry
def subreddit_subscribers(subreddit: praw.models.Subreddit) -> int:
    return subreddit.subscribers

@praw_retry
def subreddit_active_users(subreddit: praw.models.Subreddit) -> int:
    return subreddit.active_user_count


@praw_retry
def subreddit_mods_info(subreddit: praw.models.Subreddit) -> list:
    return [{'user_name': user_name(mod),
             'mod_join_date': user_mod_join_date(mod),
             'mod_permissions': user_mod_permissions(mod)} for mod in subreddit.moderator()]


@praw_retry
def user_name(user: praw.models.Redditor) -> str:
    return user.name


@praw_retry
def user_mod_permissions(user: praw.models.Redditor) -> list:
    return user.mod_permissions


@praw_retry
def user_mod_join_date(user: praw.models.Redditor) -> int:
    # Returns the UTC UNIX timestamp at which the user became moderator.
    # The redditor object should come from subreddit.moderator() call or else this won't be defined.
    return user.date


@praw_retry
def user_subreddits_moderated(user: praw.models.Redditor) -> list:
    return [subreddit_name(sr) for sr in user.moderated()]


@praw_retry
def user_name(user: praw.models.Redditor) -> str:
    return user.name


@praw_retry
def user_link_karma(user: praw.models.Redditor | praw.models.Comment | praw.models.Submission) -> int:
    if isinstance(user, praw.models.Redditor):
        return user.link_karma
    elif isinstance(user, praw.models.Comment) or isinstance(user, praw.models.Submission):
        return user.author.link_karma
    return user.link_karma


@praw_retry
def user_comment_karma(user: praw.models.Redditor | praw.models.Comment | praw.models.Submission) -> int:
    if isinstance(user, praw.models.Redditor):
        return user.comment_karma
    elif isinstance(user, praw.models.Comment) or isinstance(user, praw.models.Submission):
        return user.author.comment_karma
    return user.comment_karma


@praw_retry
def user_created_unix(user: praw.models.Redditor | praw.models.Comment | praw.models.Submission) -> int:
    if isinstance(user, praw.models.Redditor):
        return user.created_utc
    elif isinstance(user, praw.models.Comment) or isinstance(user, praw.models.Submission):
        return user.author.created_utc
    return user.created_utc


@praw_retry
def user_average_comment_embedding(user: praw.models.Redditor, limit: int = 100,
                                   model_name: str = 'minishlab/potion-base-2M') -> list:
    comments = user.comments.new(limit=limit)
    sentences = []
    for comment in comments:
        sentences.append(post_body(comment))

    if len(sentences) == 0:
        return []

    #model = SingletonSentenceTransformer(model_name).model
    model = SingletonSentenceTransformer(model_name).model

    embeddings = model.encode(sentences)

    return np.mean(embeddings, axis=0).tolist()
