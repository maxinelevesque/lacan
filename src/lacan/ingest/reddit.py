"""Routines for ingesting datasets from Reddit"""

##
# Imports

# Standard
from dataclasses import dataclass

from datetime import datetime, timezone
from pathlib import Path

# External
import convokit as ck
import atdata
import webdataset as wds
from tqdm import tqdm

# Typing
from typing import (
    Sequence,
    Iterable,
    #
    TypeAlias,
)

Pathlike: TypeAlias = Path | str


##
# Sample classes

@dataclass
class RedditPost( atdata.PackableSample ):
    """A Reddit post, as archived in the `convokit` corpus"""
    ##

    # Rerquired
    post_id: str
    """`convokit` ID of this post"""
    root_id: str
    """`convokit` ID of the root post (OP) for this thread"""
    speaker_id: str
    """Reddit username of the speaker"""
    # TODO VV - Double check with how `ck` handles this type-wise
    timestamp: str
    """Timestamp of when this post was posted"""
    text: str
    """Post text"""

    # Optional
    reply_to: str | None
    """The `convokit` ID of the post that this post follows in its thread"""
    score: int | None
    """Net score (upvotes - downvotes) at the time the `convokit` corpus was frozen"""

    ##
    # Converters

    @classmethod
    def from_convokit( cls, u: ck.Utterance ) -> 'RedditPost':
        """Extract from a `convokit.Utterance`"""
        return cls(
            post_id = u.id,
            root_id = u.conversation_id,
            speaker_id = u.speaker.id,
            timestamp = (
                datetime.fromtimestamp( u.timestamp, tz = timezone.utc )
                    .isoformat()
            ),
            text = u.text,
            reply_to = u.reply_to,
            #
            score = (
                None if 'score' not in u.meta
                else u.meta['score']
            ),
        )


##
# Routines

def iter_posts( subreddit: str, **kwargs ) -> Iterable[RedditPost]:
    """Iterate over posts in the given `subreddit`
    
    (`kwargs` are passed to `convokit.download`)"""

    ##

    verbose: bool = kwargs.get( 'verbose', False )


    if verbose:
        print( 'Downloading from `convokit` backend:' )

    cur_filename = ck.download( f'subreddit-{subreddit}', **kwargs )

    corpus = ck.Corpus(
        filename = cur_filename
    )
    
    for u in corpus.iter_utterances():
        yield RedditPost.from_convokit( u )
    
    return

_KIBIBYTE = 2 ** 10
_MEBIBYTE = 2 ** 20

_MEGABYTE = 10 ** 6

def export_subreddit( subreddit: str, to: Pathlike,
            shard_size: int = 40,
            compressed: bool = True,
            **kwargs
        ):
    """TODO"""

    # Normalize args
    to = Path( to )
    verbose: bool = kwargs.get( 'verbose', True )

    # TODO I have no idea why the conversion is needed
    shard_size_bytes = shard_size * _MEGABYTE // 8

    # Write dataset
    output_dir = to.parent
    output_stem = to.stem

    extension = '.tar.gz' if compressed else '.tar'

    output_pattern = (
        output_dir
        / f'{output_stem}-%06d{extension}'
    ).as_posix()

    output_dir.mkdir( parents = True, exist_ok = True )

    with wds.ShardWriter(
        pattern = output_pattern,
        maxsize = shard_size_bytes,
    ) as sink:
        it = iter_posts( subreddit, **kwargs )
        if verbose:
            it = tqdm( it )
        
        for sample in it:
            sink.write( sample.as_wds )


#