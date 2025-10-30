import praw
from typing import List, Dict, Optional
from tqdm import tqdm
import time
from .base_collector import BaseCollector
from utils.config import Config

class RedditCollector(BaseCollector):
    """Reddit comments collector"""
    
    def __init__(self):
        super().__init__('reddit')
        self.reddit = None
    
    def authenticate(self):
        """Authenticate with Reddit API (read-only)"""
        try:
            if not all([Config.REDDIT_CLIENT_ID, Config.REDDIT_CLIENT_SECRET, Config.REDDIT_USER_AGENT]):
                raise ValueError("Reddit API credentials incomplete")
                
            self.reddit = praw.Reddit(
                client_id=Config.REDDIT_CLIENT_ID,
                client_secret=Config.REDDIT_CLIENT_SECRET, 
                user_agent=Config.REDDIT_USER_AGENT,
            )
            
            print("✅ Reddit API authenticated successfully (read-only)")
            return True
            
        except Exception as e:
            print(f"❌ Reddit authentication failed: {e}")
            return False
    
    def collect_by_id(self, post_id: str, limit: int = 100) -> List[Dict]:
        """Collect comments from a specific post safely"""
        if not self.reddit:
            if not self.authenticate():
                return []

        print(f"🎯 Reddit direct mode for post: {post_id}")

        try:
            submission = self.reddit.submission(id=post_id)

            # Quick existence check
            if submission.removed_by_category:# or submission.over_18:
                print(f"⚠️ Post {post_id} is removed, restricted, or NSFW. Skipping.")
                return []

            submission.comments.replace_more(limit=0)
            comment_list = submission.comments.list()[:limit]

            comments = []
            for comment in tqdm(comment_list, desc=f"📝 Post {post_id[:8]}..."):
                if hasattr(comment, 'body') and comment.body not in ['[deleted]', '[removed]']:
                    comment_data = {'source': 'reddit', 'id': comment.id, 'text_raw': comment.body, 'user': str(comment.author) if comment.author else '[deleted]', 'created_at': comment.created_utc, 'score': comment.score, 'thread_id': post_id, 'parent_id': comment.parent_id, 'is_submitter': comment.is_submitter, 'subreddit': str(comment.subreddit), 'collection_method': 'direct_post_id', 'collection_timestamp': time.time()}
                    comments.append(comment_data)

            self.collected_data.extend(comments)
            return comments

        except praw.exceptions.PRAWException as e:
            print(f"❌ Reddit API error for post {post_id}: {e}")
        except Exception as e:
            print(f"❌ Unexpected error getting comments for post {post_id}: {e}")

        return []

    def collect_by_keywords(self, keywords: List[str], limit: int = 100, **kwargs) -> List[Dict]:
        """Collect comments by searching keywords in subreddit"""
        subreddit = kwargs.get('subreddit', 'all')
        
        if not self.reddit:
            if not self.authenticate():
                return []
        
        print(f"🎯 Reddit search mode in r/{subreddit} with keywords: {keywords}")
        comments = self._search_and_collect(subreddit, keywords, limit)
        
        # Update collection method
        for comment in comments:
            comment['collection_method'] = 'keyword_search'
            comment['search_keywords'] = keywords
            comment['search_subreddit'] = subreddit
            
        self.collected_data.extend(comments)
        return comments

    def _get_post_comments(self, post_id: str, max_comments: int = 100) -> List[Dict]:
        """Get comments from a specific post"""
        comments = []
        
        try:
            submission = self.reddit.submission(id=post_id)
            submission.comments.replace_more(limit=0)
            
            comment_list = submission.comments.list()[:max_comments]
            
            for comment in tqdm(comment_list, desc=f"📝 Post {post_id[:8]}..."):
                if hasattr(comment, 'body') and comment.body not in ['[deleted]', '[removed]']:
                    comment_data = {
                        'source': 'reddit',
                        'id': comment.id,
                        'text_raw': comment.body,
                        'user': str(comment.author) if comment.author else '[deleted]',
                        'created_at': comment.created_utc,
                        'score': comment.score,
                        'thread_id': post_id,
                        'parent_id': comment.parent_id,
                        'is_submitter': comment.is_submitter,
                        'subreddit': str(comment.subreddit),
                        'collection_timestamp': time.time()
                    }
                    comments.append(comment_data)
                    
        except Exception as e:
            print(f"❌ Error getting comments for post {post_id}: {e}")
        
        return comments

    def _search_and_collect(self, subreddit_name: str, keywords: List[str], limit: int = 100) -> List[Dict]:
        """Search for posts by keywords and collect comments"""
        posts = self._search_subreddit_posts(subreddit_name, keywords, limit // 10)
        print(f"🔍 Found {len(posts)} posts matching keywords")
        
        comments = []
        comments_per_post = max(1, limit // len(posts)) if posts else 0
        
        for post in posts:
            if len(comments) >= limit:
                break
            post_comments = self._get_post_comments(post['id'], comments_per_post)
            comments.extend(post_comments)
            print(f"  ✅ Post {post['id'][:8]}: {len(post_comments)} comments")
            
        return comments

    def _search_subreddit_posts(self, subreddit_name: str, keywords: List[str], max_posts: int = 50) -> List[Dict]:
        """Search for posts in a subreddit by keywords"""
        posts_data = []
        
        try:
            subreddit = self.reddit.subreddit(subreddit_name)
            
            for keyword in keywords:
                print(f"🔍 Searching r/{subreddit_name} for: '{keyword}'")
                
                posts = list(subreddit.search(keyword, limit=max_posts // len(keywords)))
                
                for post in posts:
                    if post.selftext not in ['[deleted]', '[removed]']:
                        post_data = {
                            'source': 'reddit',
                            'id': post.id,
                            'text_raw': f"{post.title} {post.selftext}".strip(),
                            'user': str(post.author) if post.author else '[deleted]',
                            'created_at': post.created_utc,
                            'score': post.score,
                            'num_comments': post.num_comments,
                            'subreddit': subreddit_name,
                            'is_post': True,
                            'collection_timestamp': time.time()
                        }
                        posts_data.append(post_data)
                        print(f"  📝 Found: {post.title[:60]}...")
                    
        except Exception as e:
            print(f"❌ Error searching r/{subreddit_name}: {e}")
        
        return posts_data