import more_itertools
from typing import BinaryIO

import click
import ijson
import tweepy

from semiphemeral.db import Tweet


def header_size(stream: BinaryIO, separator: bytes = b'[') -> int:
    buffer = bytearray()
    while True:
        chunk = stream.read(30)
        if not chunk:
            return 0
        buffer += chunk
        try:
            idx = buffer.index(separator)
        except ValueError:
            continue
        else:
            if idx:
                return idx


def paged_iterable(f):
    return more_itertools.grouper(ijson.items(f, "item.tweet.id"), n=200)


class TwitterArchive:
    def __init__(self, common, twitter) -> None:
        self.common = common
        self.twitter = twitter

    def import_twitterarchive(self, filename: str) -> None:
        with open(filename, "rb") as f:
            f.seek(header_size(f))

            import_count = 0
            for page in paged_iterable(f):
                unknown_status_ids = filter(self.is_unknown_tweet, page)
                statuses = map(self.retrieve_status, unknown_status_ids)
                import_count += self.twitter.import_statuses(statuses)

            click.echo("Imported {} tweets from Twitter Archive.".format(import_count))

    def is_unknown_tweet(self, status_id) -> bool:
        return status_id and not self.common.session.query(Tweet).filter_by(status_id=status_id).first()

    def retrieve_status(self, status_id):
        try:
            return self.twitter.api.get_status(status_id, tweet_mode="extended")
        except tweepy.error.TweepError as e:
            click.echo("Error for tweet {}: {}".format(status_id, e))