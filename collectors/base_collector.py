from abc import ABC, abstractmethod
import pandas as pd
from typing import List, Dict, Optional
from tqdm import tqdm
import json
from datetime import datetime

class BaseCollector(ABC):
    """Abstract base class for all data collectors"""
    
    def __init__(self, platform: str):
        self.platform = platform
        self.collected_data = []
    
    @abstractmethod
    def authenticate(self):
        """Authenticate with the platform API"""
        pass
    
    @abstractmethod
    def collect_by_id(self, source_id: str, limit: int = 100) -> List[Dict]:
        """Collect comments from a specific ID (video, tweet, post)"""
        pass
    
    @abstractmethod
    def collect_by_keywords(self, keywords: List[str], limit: int = 100, **kwargs) -> List[Dict]:
        """Collect comments by searching keywords"""
        pass
    
    def save_data(self, filename: str, format: str = 'jsonl'):
        """Save collected data to file"""
        if not self.collected_data:
            print(f"⚠️ No data collected from {self.platform}")
            return False
            
        df = pd.DataFrame(self.collected_data)
        
        try:
            if format == 'jsonl':
                df.to_json(filename, orient='records', lines=True, force_ascii=False, indent=2)
            elif format == 'csv':
                df.to_csv(filename, index=False, encoding='utf-8')
            elif format == 'parquet':
                df.to_parquet(filename, index=False)
            else:
                raise ValueError(f"Unsupported format: {format}")
                
            print(f"✅ Saved {len(self.collected_data)} items to {filename}")
            return True
            
        except Exception as e:
            print(f"❌ Error saving data: {e}")
            return False
    
    def get_stats(self) -> Dict:
        """Get collection statistics"""
        sources = set()
        for item in self.collected_data:
            source_id = item.get('video_id') or item.get('tweet_id') or item.get('thread_id') or 'unknown'
            sources.add(source_id)
            
        return {
            'platform': self.platform,
            'total_collected': len(self.collected_data),
            'unique_sources': len(sources),
            'timestamp': datetime.now().isoformat()
        }
    
    def clear_data(self):
        """Clear collected data from memory"""
        self.collected_data.clear()
        print(f"🧹 Cleared data from {self.platform}")
    
    def print_sample(self, n: int = 3):
        """Print sample of collected data"""
        if not self.collected_data:
            print("No data to display")
            return
            
        print(f"\n📋 Sample of {len(self.collected_data)} items from {self.platform}:")
        for i, item in enumerate(self.collected_data[:n]):
            print(f"  {i+1}. {item.get('text_raw', '')[:80]}...")