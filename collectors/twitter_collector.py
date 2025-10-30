import tweepy
from typing import List, Dict, Optional
from tqdm import tqdm
import time
from .base_collector import BaseCollector
from utils.config import Config

class TwitterCollector(BaseCollector):
    """Twitter/X comments collector"""
    
    def __init__(self):
        super().__init__('twitter')
        self.api = None
    
    def authenticate(self):
        """Authenticate with Twitter API using OAuth1"""
        try:
            if not all([Config.TWITTER_API_KEY, Config.TWITTER_API_SECRET, 
                       Config.TWITTER_ACCESS_TOKEN, Config.TWITTER_ACCESS_SECRET]):
                raise ValueError("Twitter API credentials incomplete")
                
            auth = tweepy.OAuthHandler(
                Config.TWITTER_API_KEY, 
                Config.TWITTER_API_SECRET
            )
            auth.set_access_token(
                Config.TWITTER_ACCESS_TOKEN,
                Config.TWITTER_ACCESS_SECRET
            )
            self.api = tweepy.API(auth, wait_on_rate_limit=True)
            print("✅ Twitter API authenticated successfully")
            return True
            
        except Exception as e:
            print(f"❌ Twitter authentication failed: {e}")
            return False

    def collect_by_id(self, source_id: str, limit: int = 100, id_type: str = 'username') -> List[Dict]:
        """Collect from specific ID (username or tweet_id)"""
        if not self.api:
            if not self.authenticate():
                return []
        
        comments = []
        
        try:
            if id_type == 'username':
                print(f"🎯 Twitter user mode: @{source_id}")
                comments = self._get_user_tweets(source_id, limit)
                for comment in comments:
                    comment['collection_method'] = 'direct_username'
                    
            elif id_type == 'tweet_id':
                print(f"🎯 Twitter tweet mode: {source_id}")
                comments = self._get_tweet_replies(source_id, limit)
                for comment in comments:
                    comment['collection_method'] = 'direct_tweet_id'
                    
            else:
                print(f"❌ Invalid Twitter ID type: {id_type}")
                return []
                
        except Exception as e:
            print(f"❌ Error during Twitter ID collection: {e}")
            return []
        
        self.collected_data.extend(comments)
        return comments

    def collect_by_keywords(self, keywords: List[str], limit: int = 100, **kwargs) -> List[Dict]:
        """Collect tweets by searching keywords"""
        if not self.api:
            if not self.authenticate():
                return []
        
        print(f"🎯 Twitter search mode with keywords: {keywords}")
        comments = self._search_tweets_by_keywords(keywords, limit)
        
        # Update collection method
        for comment in comments:
            comment['collection_method'] = 'keyword_search'
            comment['search_keywords'] = keywords
            
        self.collected_data.extend(comments)
        return comments

    def _get_tweet_replies(self, tweet_id: str, max_replies: int = 100) -> List[Dict]:
        """Get replies to a specific tweet"""
        replies = []
        
        try:
            original_tweet = self.api.get_status(tweet_id, tweet_mode='extended')
            conversation_id = original_tweet.id_str
            
            print(f"🔍 Searching for replies to tweet {tweet_id}...")
            
            query = f"conversation_id:{conversation_id}"
            
            for tweet in tqdm(tweepy.Cursor(self.api.search_tweets, 
                                          q=query,
                                          tweet_mode='extended',
                                          count=100).items(max_replies),
                            total=max_replies, desc="Replies"):
                
                if (tweet.in_reply_to_status_id_str == tweet_id or 
                    tweet.in_reply_to_user_id_str == original_tweet.user.id_str):
                    
                    tweet_data = {
                        'source': 'twitter',
                        'id': tweet.id_str,
                        'tweet_id': tweet_id,
                        'text_raw': tweet.full_text,
                        'user': tweet.user.screen_name,
                        'created_at': tweet.created_at.isoformat() if tweet.created_at else None,
                        'likes': tweet.favorite_count,
                        'retweets': tweet.retweet_count,
                        'reply_count': getattr(tweet, 'reply_count', 0),
                        'is_reply': True,
                        'collection_timestamp': time.time()
                    }
                    replies.append(tweet_data)
                
        except tweepy.TweepyException as e:
            print(f"❌ Error getting replies for tweet {tweet_id}: {e}")
        except Exception as e:
            print(f"❌ Unexpected error for tweet {tweet_id}: {e}")
        
        return replies

    def _search_tweets_by_keywords(self, keywords: List[str], max_tweets: int = 100) -> List[Dict]:
        """Search for tweets by keywords"""
        tweets = []
        
        for keyword in keywords:
            try:
                print(f"🔍 Searching Twitter for: '{keyword}'")
                
                for tweet in tqdm(tweepy.Cursor(self.api.search_tweets,
                                              q=keyword,
                                              tweet_mode='extended',
                                              lang='ar',
                                              count=100).items(max_tweets),
                                total=max_tweets, desc=f"'{keyword}'"):
                    
                    tweet_data = {
                        'source': 'twitter', 
                        'id': tweet.id_str,
                        'text_raw': tweet.full_text,
                        'user': tweet.user.screen_name,
                        'created_at': tweet.created_at.isoformat() if tweet.created_at else None,
                        'likes': tweet.favorite_count,
                        'retweets': tweet.retweet_count,
                        'reply_count': getattr(tweet, 'reply_count', 0),
                        'is_reply': False,
                        'collection_timestamp': time.time()
                    }
                    tweets.append(tweet_data)
                    
            except tweepy.TweepyException as e:
                print(f"❌ Error searching for '{keyword}': {e}")
            except Exception as e:
                print(f"❌ Unexpected error searching '{keyword}': {e}")
        
        return tweets

    def _get_user_tweets(self, username: str, max_tweets: int = 100) -> List[Dict]:
        """Get tweets from a specific user"""
        tweets = []
        
        try:
            print(f"🔍 Getting tweets from user: {username}")
            
            for tweet in tqdm(tweepy.Cursor(self.api.user_timeline,
                                          screen_name=username,
                                          tweet_mode='extended',
                                          count=100,
                                          exclude_replies=False,
                                          include_rts=False).items(max_tweets),
                            total=max_tweets, desc=f"@{username}"):
                
                tweet_data = {
                    'source': 'twitter',
                    'id': tweet.id_str,
                    'text_raw': tweet.full_text,
                    'user': tweet.user.screen_name,
                    'created_at': tweet.created_at.isoformat() if tweet.created_at else None,
                    'likes': tweet.favorite_count,
                    'retweets': tweet.retweet_count,
                    'reply_count': getattr(tweet, 'reply_count', 0),
                    'is_reply': tweet.in_reply_to_status_id is not None,
                    'collection_timestamp': time.time()
                }
                tweets.append(tweet_data)
                
        except tweepy.TweepyException as e:
            print(f"❌ Error getting tweets from user {username}: {e}")
        except Exception as e:
            print(f"❌ Unexpected error for user {username}: {e}")
        
        return tweets