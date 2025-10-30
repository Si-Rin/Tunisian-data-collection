#!/usr/bin/env python3
"""
Tunisian Arabic Data Collection - Enhanced CLI with ID vs Keyword Modes
"""

import argparse
import sys
import json
from pathlib import Path
from datetime import datetime

# Add current directory to path for imports
sys.path.append(str(Path(__file__).parent))

from collectors.youtube_collector import YouTubeCollector
from collectors.twitter_collector import TwitterCollector
from collectors.reddit_collector import RedditCollector
from utils.config import Config

def setup_argparse():
    """Setup enhanced command line argument parser"""
    parser = argparse.ArgumentParser(
        description='Collect data from social media platforms for Tunisian Arabic emotion detection',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # YouTube - Collect from specific video ID
  python collect.py --platform youtube --mode id --video-id "ABC123def456" --limit 500

  # YouTube - Search by keywords
  python collect.py --platform youtube --mode keywords --keywords "تونس" "tunisia" --limit 1000

  # Twitter - Collect from user timeline
  python collect.py --platform twitter --mode id --username "tunisia_user" --limit 200

  # Twitter - Collect replies to tweet
  python collect.py --platform twitter --mode id --tweet-id "123456789" --limit 100

  # Twitter - Search by keywords
  python collect.py --platform twitter --mode keywords --keywords "برشا" "ياوي" --limit 500

  # Reddit - Collect from specific post
  python collect.py --platform reddit --mode id --post-id "abc123" --limit 300

  # Reddit - Search in subreddit
  python collect.py --platform reddit --mode keywords --keywords "نحبطو" "لولاد" --subreddit "Tunisia" --limit 500

  # Collect from all platforms with keywords
  python collect.py --platform all --mode keywords --keywords "تونس" --limit 200
        """
    )
    
    # Basic arguments
    parser.add_argument(
        '--platform', 
        choices=['youtube', 'twitter', 'reddit', 'all'],
        required=True,
        help='Platform to collect data from'
    )
    
    parser.add_argument(
        '--mode',
        choices=['id', 'keywords'],
        required=True,
        help='Collection mode: "id" for specific ID, "keywords" for search'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        default=100,
        help='Maximum number of comments to collect (default: 100)'
    )
    
    parser.add_argument(
        '--output-format',
        choices=['jsonl', 'csv', 'parquet'],
        default='jsonl',
        help='Output file format (default: jsonl)'
    )
    
    # ID mode arguments
    id_group = parser.add_argument_group('ID Mode Arguments')
    id_group.add_argument(
        '--video-id',
        help='YouTube video ID (for YouTube platform)'
    )
    id_group.add_argument(
        '--username',
        help='Twitter username (for Twitter platform)'
    )
    id_group.add_argument(
        '--tweet-id',
        help='Twitter tweet ID (for Twitter platform)'
    )
    id_group.add_argument(
        '--post-id',
        help='Reddit post ID (for Reddit platform)'
    )
    
    # Keywords mode arguments
    keywords_group = parser.add_argument_group('Keywords Mode Arguments')
    keywords_group.add_argument(
        '--keywords',
        nargs='+',
        help='Keywords for search-based collection'
    )
    keywords_group.add_argument(
        '--subreddit',
        default='all',
        help='Subreddit to search in (for Reddit platform, default: all)'
    )
    
    return parser

def validate_args(args):
    """Validate command line arguments"""
    errors = []
    
    # Platform-specific validation for ID mode
    if args.mode == 'id':
        if args.platform == 'youtube' and not args.video_id:
            errors.append("--video-id required for YouTube in ID mode")
        elif args.platform == 'twitter' and not args.username and not args.tweet_id:
            errors.append("--username or --tweet-id required for Twitter in ID mode")
        elif args.platform == 'reddit' and not args.post_id:
            errors.append("--post-id required for Reddit in ID mode")
    
    # Platform-specific validation for keywords mode
    if args.mode == 'keywords':
        if not args.keywords:
            errors.append("--keywords required for keywords mode")
        if args.platform == 'reddit' and not args.subreddit:
            errors.append("--subreddit recommended for Reddit keywords mode")
    
    if errors:
        return False, errors
    
    return True, []

def get_collector(platform):
    """Get the appropriate collector instance"""
    collectors = {
        'youtube': YouTubeCollector,
        'twitter': TwitterCollector,
        'reddit': RedditCollector
    }
    return collectors[platform]()

def generate_output_filename(platform, mode, args):
    """Generate an output filename with timestamp"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if mode == 'id':
        if platform == 'youtube' and args.video_id:
            identifier = args.video_id[:8]
        elif platform == 'twitter' and args.username:
            identifier = args.username
        elif platform == 'twitter' and args.tweet_id:
            identifier = args.tweet_id[:8]
        elif platform == 'reddit' and args.post_id:
            identifier = args.post_id[:8]
        else:
            identifier = 'unknown'
    else:  # keywords mode
        identifier = '_'.join(args.keywords[:2]) if args.keywords else 'search'
        if platform == 'reddit' and args.subreddit:
            identifier = f"{args.subreddit}_{identifier}"
    
    return f"data_{platform}_{mode}_{identifier}_{timestamp}.{args.output_format}"

def collect_data(collector, platform, args):
    """Perform data collection based on platform and mode"""
    try:
        if args.mode == 'id':
            # ID-based collection
            if platform == 'youtube':
                return collector.collect_by_id(args.video_id, args.limit)
            elif platform == 'twitter':
                if args.username:
                    return collector.collect_by_id(args.username, args.limit, id_type='username')
                else:
                    return collector.collect_by_id(args.tweet_id, args.limit, id_type='tweet_id')
            elif platform == 'reddit':
                return collector.collect_by_id(args.post_id, args.limit)
                
        else:  # keywords mode
            # Keyword-based collection
            kwargs = {}
            if platform == 'reddit':
                kwargs['subreddit'] = args.subreddit
                
            return collector.collect_by_keywords(args.keywords, args.limit, **kwargs)
            
    except Exception as e:
        print(f"❌ Collection error for {platform}: {e}")
        return []

def main():
    """Main CLI entry point"""
    parser = setup_argparse()
    args = parser.parse_args()
    
    # Validate arguments
    is_valid, errors = validate_args(args)
    if not is_valid:
        print("❌ Argument errors:")
        for error in errors:
            print(f"   - {error}")
        print("\nUse --help for usage information")
        sys.exit(1)
    
    # Validate configuration
    try:
        Config.validate_config()
    except ValueError as e:
        print(f"❌ Configuration error: {e}")
        print("Please check your .env file and ensure all API keys are set")
        sys.exit(1)
    
    # Create output directory
    Path("collected_data").mkdir(exist_ok=True)
    
    # Determine platforms to collect from
    platforms = [args.platform] if args.platform != 'all' else ['youtube', 'twitter', 'reddit']
    
    all_stats = []
    
    for platform in platforms:
        print(f"\n{'='*60}")
        print(f"🔄 Collecting from {platform.upper()} - {args.mode.upper()} mode")
        print(f"{'='*60}")
        
        try:
            collector = get_collector(platform)
            
            # Perform collection
            collected_count = len(collect_data(collector, platform, args))
            
            if collected_count > 0:
                # Save data
                output_file = generate_output_filename(platform, args.mode, args)
                output_path = f"collected_data/{output_file}"
                
                collector.save_data(output_path, format=args.output_format)
                
                # Collect stats
                stats = collector.get_stats()
                all_stats.append(stats)
                print(f"📊 {platform}: {stats['total_collected']} items collected")
            else:
                print(f"⚠️ No data collected from {platform}")
            
        except Exception as e:
            print(f"❌ Error collecting from {platform}: {e}")
            continue
    
    # Print final summary
    if all_stats:
        print(f"\n{'='*60}")
        print("🎉 COLLECTION SUMMARY")
        print(f"{'='*60}")
        
        total_collected = sum(stats['total_collected'] for stats in all_stats)
        print(f"Total items collected: {total_collected}")
        
        for stats in all_stats:
            print(f"  {stats['platform']}: {stats['total_collected']} items")
    else:
        print("\n❌ No data was collected from any platform")

if __name__ == "__main__":
    main()