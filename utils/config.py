# utils/config.py
import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    """Configuration manager for all API credentials"""
    
    # YouTube
    YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')
    
    # Twitter/X
    TWITTER_API_KEY = os.getenv('TWITTER_API_KEY')
    TWITTER_API_SECRET = os.getenv('TWITTER_API_SECRET') 
    TWITTER_ACCESS_TOKEN = os.getenv('TWITTER_ACCESS_TOKEN')
    TWITTER_ACCESS_SECRET = os.getenv('TWITTER_ACCESS_SECRET')
    
    # Reddit
    REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
    REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
    REDDIT_USER_AGENT = os.getenv('REDDIT_USER_AGENT')
    
    @classmethod
    def validate_config(cls):
        """Validate that all required environment variables are set"""
        missing = []
        
        if not cls.YOUTUBE_API_KEY:
            missing.append('YOUTUBE_API_KEY')
            
        twitter_creds = [cls.TWITTER_API_KEY, cls.TWITTER_API_SECRET, 
                        cls.TWITTER_ACCESS_TOKEN, cls.TWITTER_ACCESS_SECRET]
        if not all(twitter_creds):
            missing.extend(['TWITTER_API_KEY', 'TWITTER_API_SECRET', 
                          'TWITTER_ACCESS_TOKEN', 'TWITTER_ACCESS_SECRET'])
            
        reddit_creds = [cls.REDDIT_CLIENT_ID, cls.REDDIT_CLIENT_SECRET, cls.REDDIT_USER_AGENT]
        if not all(reddit_creds):
            missing.extend(['REDDIT_CLIENT_ID', 'REDDIT_CLIENT_SECRET', 'REDDIT_USER_AGENT'])
            
        if missing:
            raise ValueError(f"Missing environment variables: {', '.join(missing)}")