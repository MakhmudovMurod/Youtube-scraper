import os
import time
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from tqdm import tqdm
from datetime import datetime

YOUTUBE_API_KEY = "YOUR_API_KEY_HERE"  # <-- PASTE YOUR API KEY HERE

MIN_SUBSCRIBERS = 1000      # 1K minimum
MAX_SUBSCRIBERS = 5000000    # 5M maximum

# Target count per category (total = 400)
CATEGORY_TARGETS = {
    "Tech & AI Content Creators": 50,
    "Filmmakers & Cinematographers": 50,
    "Video Editors & Post-Production Specialists": 40,
    "YouTube Educators & Tutorial Creators": 50,
    "Animation & Motion Graphics Artists": 30,
    "Podcast Clippers & Content Repurposers": 50,
    "Short-Form Content Specialists": 40,
    "Gaming Content Creators": 30,
    "Business & Marketing Educators": 40,
    "Music Video Producers & Digital Artists": 20
}

CATEGORIES = {
    "Tech & AI Content Creators": [
        "AI tools review", "tech creator", "AI video tutorial",
        "generative AI", "machine learning creator", "tech reviewer",
        "AI news", "tech tips", "software review"
    ],
    "Filmmakers & Cinematographers": [
        "filmmaker tips", "cinematography tutorial", "video production",
        "film editing", "indie filmmaker", "short film creator",
        "documentary filmmaker", "camera techniques"
    ],
    "Video Editors & Post-Production Specialists": [
        "video editing tutorial", "premiere pro", "davinci resolve",
        "post production tips", "editing workflow", "color grading",
        "final cut pro", "video editor"
    ],
    "YouTube Educators & Tutorial Creators": [
        "educational youtube", "explainer videos", "tutorial creator",
        "how to learn", "study tips", "educational content",
        "online teacher", "knowledge channel"
    ],
    "Animation & Motion Graphics Artists": [
        "motion graphics tutorial", "animation creator", "2d animation",
        "after effects tutorial", "motion design", "animated explainer",
        "character animation", "digital animation"
    ],
    "Podcast Clippers & Content Repurposers": [
        "podcast clips", "podcast highlights", "interview clips",
        "podcast channel", "conversation highlights", "content repurposing"
    ],
    "Short-Form Content Specialists": [
        "youtube shorts creator", "short form content", "viral shorts",
        "shorts channel", "quick videos", "tiktok style"
    ],
    "Gaming Content Creators": [
        "gaming channel", "let's play", "game review",
        "gaming commentary", "indie game showcase", "gaming highlights"
    ],
    "Business & Marketing Educators": [
        "marketing tips", "business education", "entrepreneur advice",
        "startup tips", "social media marketing", "digital marketing",
        "business strategy", "online business"
    ],
    "Music Video Producers & Digital Artists": [
        "music video creator", "music producer", "digital art tutorial",
        "music visualizer", "independent artist", "beat making",
        "creative visuals", "music production"
    ]
}

# =============================================================================
# SCRIPT - Don't modify below unless you know what you're doing
# =============================================================================

class YouTubeCreatorFinder:
    def __init__(self, api_key):
        self.youtube = build('youtube', 'v3', developerKey=api_key)
        self.all_creators = []
        self.seen_channel_ids = set()
    
    def search_channels(self, query, max_results=50):
        """Search for channels based on a query"""
        channels = []
        next_page_token = None
        
        while len(channels) < max_results:
            try:
                request = self.youtube.search().list(
                    part="snippet",
                    q=query,
                    type="channel",
                    maxResults=min(50, max_results - len(channels)),
                    pageToken=next_page_token
                )
                response = request.execute()
                
                for item in response.get('items', []):
                    channel_id = item['snippet']['channelId']
                    if channel_id not in self.seen_channel_ids:
                        channels.append({
                            'channel_id': channel_id,
                            'channel_name': item['snippet']['title'],
                            'description': item['snippet']['description'][:200] if item['snippet']['description'] else ''
                        })
                        self.seen_channel_ids.add(channel_id)
                
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
                    
            except HttpError as e:
                print(f"API Error: {e}")
                break
            
            time.sleep(0.1)  # Rate limiting
        
        return channels
    
    def get_channel_stats(self, channel_ids):
        """Get detailed statistics for a list of channel IDs"""
        stats = {}
        
        # Process in batches of 50 (API limit)
        for i in range(0, len(channel_ids), 50):
            batch = channel_ids[i:i+50]
            try:
                request = self.youtube.channels().list(
                    part="statistics,snippet,contentDetails",
                    id=','.join(batch)
                )
                response = request.execute()
                
                for item in response.get('items', []):
                    channel_id = item['id']
                    statistics = item.get('statistics', {})
                    stats[channel_id] = {
                        'subscribers': int(statistics.get('subscriberCount', 0)),
                        'total_views': int(statistics.get('viewCount', 0)),
                        'video_count': int(statistics.get('videoCount', 0)),
                        'channel_url': f"https://www.youtube.com/channel/{channel_id}",
                        'custom_url': item['snippet'].get('customUrl', ''),
                        'country': item['snippet'].get('country', 'Unknown')
                    }
                    
            except HttpError as e:
                print(f"API Error getting stats: {e}")
            
            time.sleep(0.1)
        
        return stats
    
    def get_recent_videos_avg_views(self, channel_id, num_videos=10):
        """Get average views from recent videos"""
        try:
            # Get uploads playlist
            request = self.youtube.channels().list(
                part="contentDetails",
                id=channel_id
            )
            response = request.execute()
            
            if not response.get('items'):
                return 0
            
            uploads_playlist = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
            
            # Get recent videos
            request = self.youtube.playlistItems().list(
                part="contentDetails",
                playlistId=uploads_playlist,
                maxResults=num_videos
            )
            response = request.execute()
            
            video_ids = [item['contentDetails']['videoId'] for item in response.get('items', [])]
            
            if not video_ids:
                return 0
            
            # Get video statistics
            request = self.youtube.videos().list(
                part="statistics",
                id=','.join(video_ids)
            )
            response = request.execute()
            
            views = [int(item['statistics'].get('viewCount', 0)) for item in response.get('items', [])]
            
            return int(sum(views) / len(views)) if views else 0
            
        except HttpError as e:
            return 0
    
    def find_creators_for_category(self, category_name, search_terms, target_count):
        """Find creators for a specific category"""
        print(f"\n🔍 Searching category: {category_name}")
        category_creators = []
        
        for term in tqdm(search_terms, desc="Search terms"):
            if len(category_creators) >= target_count:
                break
                
            channels = self.search_channels(term, max_results=20)
            
            if not channels:
                continue
            
            # Get stats for found channels
            channel_ids = [c['channel_id'] for c in channels]
            stats = self.get_channel_stats(channel_ids)
            
            for channel in channels:
                if len(category_creators) >= target_count:
                    break
                    
                channel_id = channel['channel_id']
                if channel_id not in stats:
                    continue
                
                channel_stats = stats[channel_id]
                subs = channel_stats['subscribers']
                
                # Filter by subscriber count
                if subs < MIN_SUBSCRIBERS or subs > MAX_SUBSCRIBERS:
                    continue
                
                # Get average views (this uses extra API quota)
                avg_views = self.get_recent_videos_avg_views(channel_id)
                
                creator_data = {
                    'Category': category_name,
                    'Channel Name': channel['channel_name'],
                    'Channel URL': channel_stats['channel_url'],
                    'Custom URL': f"https://youtube.com/{channel_stats['custom_url']}" if channel_stats['custom_url'] else '',
                    'Subscribers': subs,
                    'Avg Views': avg_views,
                    'Total Views': channel_stats['total_views'],
                    'Video Count': channel_stats['video_count'],
                    'Country': channel_stats['country'],
                    'Description': channel['description'],
                    'Why This Creator': ''  # To be filled manually or with AI
                }
                
                category_creators.append(creator_data)
                time.sleep(0.2)  # Rate limiting for avg views calls
        
        print(f"✅ Found {len(category_creators)} creators in {category_name}")
        return category_creators
    
    def generate_why_reason(self, creator):
        """Generate a basic 'why' reason based on channel data"""
        subs = creator['Subscribers']
        avg_views = creator['Avg Views']
        category = creator['Category']
        
        engagement = "high" if avg_views > subs * 0.1 else "solid" if avg_views > subs * 0.05 else "growing"
        
        reasons = {
            "Tech & AI Content Creators": f"Tech-savvy creator with {engagement} engagement. Natural early adopter who educates audience about AI tools.",
            "Filmmakers & Cinematographers": f"Active filmmaker with {engagement} engagement. Natural fit for AI video tools to streamline production workflow.",
            "Video Editors & Post-Production Specialists": f"Professional editor with {engagement} credibility. Can validate AI tools for serious production pipelines.",
            "YouTube Educators & Tutorial Creators": f"Educational creator with {engagement} viewer retention. AI tools can help produce more content efficiently.",
            "Animation & Motion Graphics Artists": f"Motion designer with {engagement} audience. Perfect match for AI-powered generative video capabilities.",
            "Podcast Clippers & Content Repurposers": f"Content repurposer who needs efficient editing tools. Perfect use case for AI-powered clip generation.",
            "Short-Form Content Specialists": f"Short-form specialist where speed matters. AI tools directly impact their ability to post frequently.",
            "Gaming Content Creators": f"Gaming creator producing regular content. AI editing tools can accelerate their high-volume upload schedule.",
            "Business & Marketing Educators": f"Business-focused creator who understands ROI. Can influence other creators and demonstrate practical AI tool value.",
            "Music Video Producers & Digital Artists": f"Music creator needing visual content. AI video tools can help produce music videos and visualizers efficiently."
        }
        
        return reasons.get(category, f"Active creator with {subs:,} subscribers and {engagement} engagement rate.")
    
    def run(self, output_file="youtube_creators.xlsx"):
        """Main execution method"""
        print("=" * 60)
        print("🚀 YouTube Creator Finder for Higgsfield AI")
        print("=" * 60)
        total_target = sum(CATEGORY_TARGETS.values())
        print(f"Target: {total_target} total creators across {len(CATEGORIES)} categories")
        print(f"Subscriber range: {MIN_SUBSCRIBERS:,} - {MAX_SUBSCRIBERS:,}")
        print("=" * 60)
        
        all_creators = []
        
        for category_name, search_terms in CATEGORIES.items():
            target_count = CATEGORY_TARGETS[category_name]
            creators = self.find_creators_for_category(
                category_name, 
                search_terms, 
                target_count
            )
            
            # Generate 'why' reasons
            for creator in creators:
                creator['Why This Creator'] = self.generate_why_reason(creator)
            
            all_creators.extend(creators)
            print(f"📊 Total so far: {len(all_creators)} creators")
        
        # Create DataFrame and save
        df = pd.DataFrame(all_creators)
        
        # Reorder columns for better readability
        column_order = [
            'Category', 'Channel Name', 'Channel URL', 'Custom URL',
            'Subscribers', 'Avg Views', 'Video Count', 'Country',
            'Why This Creator', 'Description'
        ]
        df = df[[col for col in column_order if col in df.columns]]
        
        # Save to Excel with formatting
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='All Creators')
            
            # Also create separate sheets per category
            for category in CATEGORIES.keys():
                category_df = df[df['Category'] == category]
                sheet_name = category[:31]  # Excel sheet name limit
                category_df.to_excel(writer, index=False, sheet_name=sheet_name)
        
        # Also save as CSV for easy access
        csv_file = output_file.replace('.xlsx', '.csv')
        df.to_csv(csv_file, index=False)
        
        print("\n" + "=" * 60)
        print("✅ COMPLETE!")
        print("=" * 60)
        print(f"📁 Excel file: {output_file}")
        print(f"📁 CSV file: {csv_file}")
        print(f"📊 Total creators found: {len(df)}")
        print("\n📈 Breakdown by category:")
        print(df['Category'].value_counts().to_string())
        
        return df


def main():
    if YOUTUBE_API_KEY == "YOUR_API_KEY_HERE":
        print("❌ ERROR: Please set your YouTube API key!")
        print("\n📋 How to get an API key:")
        print("1. Go to: https://console.cloud.google.com/")
        print("2. Create a new project (or select existing)")
        print("3. Go to 'APIs & Services' > 'Library'")
        print("4. Search for 'YouTube Data API v3' and enable it")
        print("5. Go to 'APIs & Services' > 'Credentials'")
        print("6. Click 'Create Credentials' > 'API Key'")
        print("7. Copy the key and paste it in this script (YOUTUBE_API_KEY variable)")
        return
    
    finder = YouTubeCreatorFinder(YOUTUBE_API_KEY)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"higgsfield_creators_{timestamp}.xlsx"
    
    df = finder.run(output_file)
    
    print("\n🎯 Next steps:")
    print("1. Review the spreadsheet and refine 'Why This Creator' column")
    print("2. Remove any irrelevant channels")
    print("3. Add any manually-found creators")
    print("4. Ensure you have at least 400 total")


if __name__ == "__main__":
    main()

