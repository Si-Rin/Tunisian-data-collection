from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from typing import List, Dict, Optional
from tqdm import tqdm
import time
from .base_collector import BaseCollector
from utils.config import Config

class YouTubeCollector(BaseCollector):
    """YouTube comments collector"""
    
    def __init__(self):
        super().__init__('youtube')
        self.youtube = None
    
    def authenticate(self):
        """Authenticate with YouTube API using API key"""
        try:
            if not Config.YOUTUBE_API_KEY:
                raise ValueError("YouTube API key not found in configuration")
                
            self.youtube = build('youtube', 'v3', developerKey=Config.YOUTUBE_API_KEY)
            print("✅ YouTube API authenticated successfully")
            return True
            
        except Exception as e:
            print(f"❌ YouTube authentication failed: {e}")
            return False

    def collect_by_id(self, video_id: str, limit: int = 100) -> List[Dict]:
        """Collect comments from a specific video ID"""
        if not self.youtube:
            if not self.authenticate():
                return []
        
        print(f"🎯 YouTube direct mode for video: {video_id}")
        comments = self._get_video_comments(video_id, limit)
        self.collected_data.extend(comments)
        return comments

    def collect_by_keywords(self, keywords: List[str], limit: int = 100, **kwargs) -> List[Dict]:
        """Collect comments by searching for videos with keywords"""
        if not self.youtube:
            if not self.authenticate():
                return []
        
        print(f"🎯 YouTube search mode with keywords: {keywords}")
        comments = self._search_and_collect(keywords, limit)
        self.collected_data.extend(comments)
        return comments

    def _get_video_comments(self, video_id: str, max_results: int = 100) -> List[Dict]:
        """Get comments from a specific video by ID"""
        comments = []
        next_page_token = None
        total_collected = 0
        
        try:
            with tqdm(total=max_results, desc=f"📹 Video {video_id[:8]}...") as pbar:
                while total_collected < max_results:
                    remaining = max_results - total_collected
                    request_max = min(100, remaining)
                    
                    request = self.youtube.commentThreads().list(
                        part='snippet',
                        videoId=video_id,
                        maxResults=request_max,
                        pageToken=next_page_token,
                        textFormat='plainText'
                    )
                    response = request.execute()
                    
                    if not response.get('items'):
                        break
                    
                    for item in response.get('items', []):
                        comment = item['snippet']['topLevelComment']['snippet']
                        comment_data = {
                            'source': 'youtube',
                            'id': item['id'],
                            'video_id': video_id,
                            'text_raw': comment['textDisplay'],
                            'user': comment['authorDisplayName'],
                            'created_at': comment['publishedAt'],
                            'likes': comment['likeCount'],
                            'reply_count': comment.get('totalReplyCount', 0),
                            'parent_id': None,
                            'collection_timestamp': time.time(),
                            'collection_method': 'direct_id'
                        }
                        comments.append(comment_data)
                        total_collected += 1
                        pbar.update(1)
                        
                        if total_collected >= max_results:
                            break
                    
                    next_page_token = response.get('nextPageToken')
                    if not next_page_token:
                        break
                        
                    time.sleep(0.1)
                        
        except HttpError as e:
            if e.resp.status == 403:
                print(f"🔒 Comments disabled for video {video_id}")
            elif e.resp.status == 404:
                print(f"❌ Video {video_id} not found")
            else:
                print(f"❌ Error getting comments for video {video_id}: {e}")
        except Exception as e:
            print(f"❌ Unexpected error for video {video_id}: {e}")
        
        return comments

    def _search_and_collect(self, keywords: List[str], limit: int = 100) -> List[Dict]:
        """Search for videos by keywords and collect comments"""
        video_ids = self._search_videos_by_keywords(keywords, max_videos=10)
        
        if not video_ids:
            print("❌ No videos found with the given keywords")
            return []
            
        print(f"📹 Processing {len(video_ids)} videos...")
        comments = []
        
        comments_per_video = max(1, limit // len(video_ids))
        
        for video_id in video_ids:
            if len(comments) >= limit:
                break
                
            video_comments = self._get_video_comments(video_id, comments_per_video)
            
            # Update collection method for search results
            for comment in video_comments:
                comment['collection_method'] = 'keyword_search'
                comment['search_keywords'] = keywords
                
            comments.extend(video_comments)
            print(f"  ✅ Video {video_id[:8]}: {len(video_comments)} comments")
            
        return comments

    def _search_videos_by_keywords(self, keywords: List[str], max_videos: int = 50) -> List[str]:
        """Search for videos by keywords and return video IDs"""
        video_ids = []
        
        for keyword in keywords:
            try:
                print(f"🔍 Searching YouTube for: '{keyword}'")
                request = self.youtube.search().list(
                    part='id,snippet',
                    q=keyword,
                    type='video',
                    maxResults=min(50, max_videos - len(video_ids)),
                    regionCode='TN',
                    relevanceLanguage='ar'
                )
                response = request.execute()
                
                for item in response.get('items', []):
                    video_id = item['id']['videoId']
                    video_title = item['snippet']['title']
                    video_ids.append(video_id)
                    print(f"  📹 Found: {video_title[:50]}... (ID: {video_id})")
                    
                    if len(video_ids) >= max_videos:
                        break
                        
                time.sleep(0.1)
                        
            except HttpError as e:
                print(f"❌ Error searching for keyword '{keyword}': {e}")
            except Exception as e:
                print(f"❌ Unexpected error searching '{keyword}': {e}")
        
        return video_ids