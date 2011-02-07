from gevent import monkey
monkey.patch_all()
from gevent.pool import Group

from django.conf import settings
from django.core.management.base import NoArgsCommand
from twittersync.models import TwitterAccount
from twittersync_oauth.helpers import TwitterSyncOAuthHelper
from twitter import Twitter, OAuth
from django.core.cache import cache

class Command(NoArgsCommand):
    help = 'Sync all active Twitter account streams.'

    def twitter_helper(self, tha):
        return TwitterSyncOAuthHelper(tha[0], tha[1]).sync_twitter_account()

    def handle_noargs(self, **options):
        tokens = settings.TWITTER_TOKENS
        num_tokens = len(tokens)
        group = Group()
        th_args=[]
        for account in TwitterAccount.active.all():
            try:
                idx = cache.incr('twitter:connection:last') % num_tokens
            except ValueError:
                from random import randint
                idx = randint(0, num_tokens - 1)
                cache.set('twitter:connection:last', idx)
            token, token_secret = tokens[idx]
            conn = Twitter(domain="api.twitter.com", api_version='1',
               auth=OAuth(token,
                          token_secret,
                          settings.TWITTER_CONSUMER_KEY,
                          settings.TWITTER_CONSUMER_SECRET,
                          )
               )
            th_args.append((account, conn))
        group.imap(self.twitter_helper, th_args)


