import os
import time
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from tqdm import tqdm
from datetime import datetime


YOUTUBE_API_KEY = ""  # <-- PASTE YOUR API KEY HERE

TIER_CONFIG = {
    "Nano (1K-10K subs)": {
        "min_subs": 1000,
        "max_subs": 10000,
        "target_count": 200,
        "percentage": 50
    },
    "Micro (10K-100K subs)": {
        "min_subs": 10000,
        "max_subs": 100000,
        "target_count": 150,
        "percentage": 37.5
    },
    "Mid-Tier (100K-500K subs)": {
        "min_subs": 100000,
        "max_subs": 500000,
        "target_count": 40,
        "percentage": 10
    },
    "Macro (500K-1M+ subs)": {
        "min_subs": 500000,
        "max_subs": 50000000,  # 50M cap
        "target_count": 10,
        "percentage": 2.5
    }
}


CATEGORIES = {
    "Tech & AI Content Creators": {
        "description": "They already review AI tools for millions of tech-savvy subscribers. Their content ranks in Google for 'AI video tools' searches.",
        "keywords": [
            # Primary AI keywords
            "AI tools review", "AI video tools", "generative AI tutorial",
            "AI creator", "AI news channel", "AI technology review",
            # Specific tool keywords
            "ChatGPT tutorial", "Midjourney tutorial", "Stable Diffusion",
            "Runway ML review", "Pika Labs", "Sora AI", "Luma AI",
            "AI video generation", "AI animation tools",
            # Creator-focused
            "AI for creators", "AI editing tools", "content creation AI",
            "AI workflow", "AI automation", "best AI tools 2024",
            "AI video editor review", "generative video AI"
        ],
        "target_per_tier": {"Nano (1K-10K subs)": 35, "Micro (10K-100K subs)": 25, "Mid-Tier (100K-500K subs)": 7, "Macro (500K-1M+ subs)": 2}
    },
    "Filmmakers & Cinematographers": {
        "description": "They teach video production to our target audience. Their professional work doubles as proof that the tool handles serious projects.",
        "keywords": [
            # Core filmmaking
            "filmmaker tips", "filmmaking tutorial", "cinematography",
            "indie filmmaker", "short film creator", "documentary filmmaker",
            "video production tutorial", "filmmaking techniques",
            # Camera & Gear
            "camera tutorial", "cinematography gear", "cinema camera",
            "DSLR filmmaking", "mirrorless cinematography",
            # Techniques
            "film lighting tutorial", "color grading filmmaker",
            "cinematic shots", "visual storytelling", "camera movement",
            "gimbal filmmaking", "cinematic vlog"
        ],
        "target_per_tier": {"Nano (1K-10K subs)": 35, "Micro (10K-100K subs)": 25, "Mid-Tier (100K-500K subs)": 7, "Macro (500K-1M+ subs)": 2}
    },
    "VFX & Visual Effects Artists": {
        "description": "Complex effects take hours to render. AI elements can replace expensive stock footage.",
        "keywords": [
            # VFX Core
            "VFX tutorial", "visual effects", "VFX breakdown",
            "VFX artist", "CGI tutorial", "special effects tutorial",
            "VFX compositing", "green screen VFX",
            # Software
            "After Effects VFX", "Blender VFX", "Nuke tutorial",
            "Houdini tutorial", "Fusion compositing",
            # Techniques
            "compositing tutorial", "rotoscoping", "tracking tutorial",
            "chroma key tutorial", "matte painting",
            "particle simulation", "explosion effects VFX"
        ],
        "target_per_tier": {"Nano (1K-10K subs)": 30, "Micro (10K-100K subs)": 25, "Mid-Tier (100K-500K subs)": 6, "Macro (500K-1M+ subs)": 1}
    },
    "Video Editors & Post-Production Specialists": {
        "description": "Agencies and studios watch what tools they use. Their technical breakdowns convince professional users.",
        "keywords": [
            # Software Specific
            "Premiere Pro tutorial", "DaVinci Resolve tutorial",
            "Final Cut Pro", "After Effects tutorial", "CapCut tutorial",
            # Editing Techniques
            "video editing tutorial", "editing workflow", "post-production tips",
            "editing techniques", "montage editing", "multi-cam editing",
            # Color & Effects
            "color grading tutorial", "color correction", "LUTs tutorial",
            "cinematic color grading",
            # Professional
            "professional video editor", "freelance editor tips",
            "video editing business", "editing portfolio"
        ],
        "target_per_tier": {"Nano (1K-10K subs)": 35, "Micro (10K-100K subs)": 25, "Mid-Tier (100K-500K subs)": 7, "Macro (500K-1M+ subs)": 2}
    },
    "Music Video Producers & Digital Artists": {
        "description": "Independent musicians need videos but can't afford production crews. They're open to AI solutions.",
        "keywords": [
            # Music Video
            "music video creator", "music video production",
            "music visualizer tutorial", "lyric video tutorial",
            "DIY music video", "independent music video",
            # Music Production
            "music producer vlog", "beat making visual",
            "music production workflow",
            # Digital Art
            "digital art tutorial", "digital artist", "digital painting",
            "procreate tutorial", "illustration tutorial",
            "concept art", "character design tutorial",
            # Creative Visuals
            "creative visuals", "motion design music", "audio reactive visuals"
        ],
        "target_per_tier": {"Nano (1K-10K subs)": 30, "Micro (10K-100K subs)": 20, "Mid-Tier (100K-500K subs)": 6, "Macro (500K-1M+ subs)": 1}
    },
    "Animation & Motion Graphics Artists": {
        "description": "Animation takes forever. They'll showcase what's possible and share it everywhere.",
        "keywords": [
            # Motion Graphics
            "motion graphics tutorial", "motion design", "After Effects motion",
            "kinetic typography", "motion designer", "mograph tutorial",
            # Animation Styles
            "2D animation tutorial", "character animation",
            "frame-by-frame animation", "digital animation",
            "animated explainer", "animation tips",
            # Software Specific
            "Cinema 4D tutorial", "Blender animation",
            "After Effects animation", "Toon Boom tutorial",
            # Specialized
            "logo animation", "text animation tutorial",
            "transitions tutorial", "lower thirds tutorial"
        ],
        "target_per_tier": {"Nano (1K-10K subs)": 35, "Micro (10K-100K subs)": 30, "Mid-Tier (100K-500K subs)": 7, "Macro (500K-1M+ subs)": 2}
    }
}


class YouTubeCreatorFinder:
    def __init__(self, api_key):
        self.youtube = build('youtube', 'v3', developerKey=api_key)
        self.all_creators = []
        self.seen_channel_ids = set()
        self.tier_counts = {tier: 0 for tier in TIER_CONFIG.keys()}
    
    def get_tier_for_subscribers(self, subscriber_count):
        """Determine which tier a channel belongs to based on subscriber count"""
        for tier_name, config in TIER_CONFIG.items():
            if config["min_subs"] <= subscriber_count < config["max_subs"]:
                return tier_name
        if subscriber_count >= 500000:
            return "Macro (500K-1M+ subs)"
        return None
    
    def format_subscriber_count(self, count):
        """Format subscriber count for display (e.g., 2.24M, 184K)"""
        if count >= 1000000:
            return f"{count/1000000:.2f}M"
        elif count >= 1000:
            return f"{count/1000:.1f}K"
        return str(count)
    
    def format_view_range(self, avg_views):
        """Format average views as a range"""
        if avg_views < 1000:
            return f"0-1K"
        elif avg_views < 5000:
            return f"1K-5K"
        elif avg_views < 10000:
            return f"5K-10K"
        elif avg_views < 20000:
            return f"10K-20K"
        elif avg_views < 50000:
            return f"20K-50K"
        elif avg_views < 100000:
            return f"50K-100K"
        elif avg_views < 300000:
            return f"100K-300K"
        elif avg_views < 500000:
            return f"300K-500K"
        elif avg_views < 1000000:
            return f"500K-1M"
        else:
            return f"1M+"
    
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
                    pageToken=next_page_token,
                    relevanceLanguage="en"  # Focus on English content
                )
                response = request.execute()
                
                for item in response.get('items', []):
                    channel_id = item['snippet']['channelId']
                    if channel_id not in self.seen_channel_ids:
                        channels.append({
                            'channel_id': channel_id,
                            'channel_name': item['snippet']['title'],
                            'description': item['snippet']['description'][:500] if item['snippet']['description'] else ''
                        })
                
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
                    
            except HttpError as e:
                print(f"API Error: {e}")
                break
            
            time.sleep(0.1)
        
        return channels
    
    def get_channel_stats(self, channel_ids):
        """Get detailed statistics for a list of channel IDs"""
        stats = {}
        
        for i in range(0, len(channel_ids), 50):
            batch = channel_ids[i:i+50]
            try:
                request = self.youtube.channels().list(
                    part="statistics,snippet,contentDetails,brandingSettings",
                    id=','.join(batch)
                )
                response = request.execute()
                
                for item in response.get('items', []):
                    channel_id = item['id']
                    statistics = item.get('statistics', {})
                    snippet = item.get('snippet', {})
                    branding = item.get('brandingSettings', {}).get('channel', {})
                    
                    # Get custom URL
                    custom_url = snippet.get('customUrl', '')
                    if custom_url:
                        channel_url = f"https://www.youtube.com/{custom_url}"
                    else:
                        channel_url = f"https://www.youtube.com/channel/{channel_id}"
                    
                    stats[channel_id] = {
                        'subscribers': int(statistics.get('subscriberCount', 0)),
                        'total_views': int(statistics.get('viewCount', 0)),
                        'video_count': int(statistics.get('videoCount', 0)),
                        'channel_url': channel_url,
                        'custom_url': custom_url,
                        'country': snippet.get('country', 'Unknown'),
                        'description': snippet.get('description', '')[:500],
                        'keywords': branding.get('keywords', ''),
                        'published_at': snippet.get('publishedAt', '')
                    }
                    
            except HttpError as e:
                print(f"API Error getting stats: {e}")
            
            time.sleep(0.1)
        
        return stats
    
    def get_recent_videos_stats(self, channel_id, num_videos=10):
        """Get statistics from recent videos including avg views"""
        try:
            # Get uploads playlist
            request = self.youtube.channels().list(
                part="contentDetails",
                id=channel_id
            )
            response = request.execute()
            
            if not response.get('items'):
                return {'avg_views': 0, 'recent_videos': []}
            
            uploads_playlist = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
            
            # Get recent videos
            request = self.youtube.playlistItems().list(
                part="contentDetails,snippet",
                playlistId=uploads_playlist,
                maxResults=num_videos
            )
            response = request.execute()
            
            video_ids = [item['contentDetails']['videoId'] for item in response.get('items', [])]
            
            if not video_ids:
                return {'avg_views': 0, 'recent_videos': []}
            
            # Get video statistics
            request = self.youtube.videos().list(
                part="statistics,snippet,contentDetails",
                id=','.join(video_ids)
            )
            response = request.execute()
            
            videos_data = []
            views = []
            for item in response.get('items', []):
                video_stats = item.get('statistics', {})
                view_count = int(video_stats.get('viewCount', 0))
                views.append(view_count)
                videos_data.append({
                    'title': item['snippet']['title'],
                    'views': view_count,
                    'likes': int(video_stats.get('likeCount', 0)),
                    'comments': int(video_stats.get('commentCount', 0)),
                    'published_at': item['snippet']['publishedAt']
                })
            
            avg_views = int(sum(views) / len(views)) if views else 0
            
            return {
                'avg_views': avg_views,
                'recent_videos': videos_data
            }
            
        except HttpError as e:
            return {'avg_views': 0, 'recent_videos': []}
    
    def check_partnership_signals(self, channel_stats, recent_videos):
        """Check for signals that indicate partnership receptiveness"""
        signals = []
        
        description = channel_stats.get('description', '').lower()
        
        # Check for business contact info
        if any(word in description for word in ['business', 'contact', 'email', 'collaboration', 'sponsor', 'partnership']):
            signals.append("Has business contact info")
        
        # Check for consistent uploads (if we have video dates)
        if recent_videos:
            signals.append(f"Active channel ({len(recent_videos)} recent videos)")
        
        # Check engagement rate
        if channel_stats.get('subscribers', 0) > 0:
            avg_views = sum(v.get('views', 0) for v in recent_videos) / len(recent_videos) if recent_videos else 0
            engagement_rate = avg_views / channel_stats['subscribers'] * 100
            if engagement_rate > 10:
                signals.append("High engagement (>10%)")
            elif engagement_rate > 5:
                signals.append("Good engagement (>5%)")
        
        return signals
    
    def generate_why_reason(self, creator_data, category, recent_videos):
        """Generate a comprehensive 'Why This Creator' reason"""
        subs = creator_data['Subscribers']
        avg_views = creator_data['Avg Views (Raw)']
        tier = creator_data['Subscriber Category']
        
        # Calculate engagement rate
        engagement_rate = (avg_views / subs * 100) if subs > 0 else 0
        
        engagement_desc = ""
        if engagement_rate > 10:
            engagement_desc = "Very high engagement rate"
        elif engagement_rate > 5:
            engagement_desc = "Strong engagement rate"
        elif engagement_rate > 2:
            engagement_desc = "Good engagement rate"
        else:
            engagement_desc = "Growing channel"
        
        # Category-specific reasons
        category_reasons = {
            "Tech & AI Content Creators": f"Reviews AI tools and technology. {engagement_desc}. Audience actively seeks AI video tools.",
            "Filmmakers & Cinematographers": f"Creates filmmaking content. {engagement_desc}. Audience would benefit from AI video generation for production.",
            "VFX & Visual Effects Artists": f"Specializes in visual effects. {engagement_desc}. AI can accelerate complex effects and replace expensive stock footage.",
            "Video Editors & Post-Production Specialists": f"Professional editing tutorials. {engagement_desc}. Can demonstrate AI tools to professional audience.",
            "Music Video Producers & Digital Artists": f"Creates music/visual content. {engagement_desc}. AI video tools can help produce music videos efficiently.",
            "Animation & Motion Graphics Artists": f"Motion graphics/animation creator. {engagement_desc}. AI can significantly speed up animation workflow."
        }
        
        base_reason = category_reasons.get(category, f"Active creator. {engagement_desc}.")
        
        # Add tier-specific note
        tier_notes = {
            "Nano (1K-10K subs)": "Highly responsive to partnerships, affordable for scale.",
            "Micro (10K-100K subs)": "Sweet spot for authentic partnerships with good reach.",
            "Mid-Tier (100K-500K subs)": "Credibility builder, validates product for larger audiences.",
            "Macro (500K-1M+ subs)": "Brand amplification and market-wide visibility."
        }
        
        return f"{base_reason} {tier_notes.get(tier, '')}"
    
    def find_creators_for_category(self, category_name, category_config):
        """Find creators for a specific category with tier distribution"""
        print(f"\n🔍 Searching category: {category_name}")
        print(f"   Target per tier: {category_config['target_per_tier']}")
        
        category_creators = []
        tier_counts_local = {tier: 0 for tier in TIER_CONFIG.keys()}
        keywords = category_config['keywords']
        target_per_tier = category_config['target_per_tier']
        
        for keyword in tqdm(keywords, desc="Keywords"):
            # Check if we've met all tier targets for this category
            all_targets_met = all(
                tier_counts_local[tier] >= target_per_tier.get(tier, 0)
                for tier in TIER_CONFIG.keys()
            )
            if all_targets_met:
                break
            
            channels = self.search_channels(keyword, max_results=30)
            
            if not channels:
                continue
            
            # Get stats for found channels
            channel_ids = [c['channel_id'] for c in channels]
            stats = self.get_channel_stats(channel_ids)
            
            for channel in channels:
                channel_id = channel['channel_id']
                
                if channel_id in self.seen_channel_ids:
                    continue
                
                if channel_id not in stats:
                    continue
                
                channel_stats = stats[channel_id]
                subs = channel_stats['subscribers']
                
                # Determine tier
                tier = self.get_tier_for_subscribers(subs)
                if tier is None:
                    continue
                
                # Check if we need more creators in this tier for this category
                if tier_counts_local[tier] >= target_per_tier.get(tier, 0):
                    continue
                
                # Get recent videos and avg views
                video_data = self.get_recent_videos_stats(channel_id)
                avg_views = video_data['avg_views']
                
                # Skip channels with very low engagement
                if avg_views < 100:
                    continue
                
                self.seen_channel_ids.add(channel_id)
                
                creator_data = {
                    'Channel name': channel['channel_name'],
                    'Link': channel_stats['channel_url'],
                    'Subscribers': self.format_subscriber_count(subs),
                    'Subscribers (Raw)': subs,
                    'Avg Views': self.format_view_range(avg_views),
                    'Avg Views (Raw)': avg_views,
                    'Content Category': category_name,
                    'Subscriber Category': tier,
                    'Why this Creator': '',  # Will be filled later
                    'Country': channel_stats['country'],
                    'Video Count': channel_stats['video_count'],
                    'Description': channel_stats['description'][:200]
                }
                
                # Generate why reason
                creator_data['Why this Creator'] = self.generate_why_reason(
                    creator_data, category_name, video_data['recent_videos']
                )
                
                category_creators.append(creator_data)
                tier_counts_local[tier] += 1
                self.tier_counts[tier] += 1
                
                time.sleep(0.3)  # Rate limiting
        
        print(f"✅ Found {len(category_creators)} creators in {category_name}")
        print(f"   Breakdown: {tier_counts_local}")
        
        return category_creators
    
    def run(self, output_file="higgsfield_creators.csv"):
        """Main execution method"""
        print("=" * 70)
        print("🚀 YouTube Creator Finder for Higgsfield AI")
        print("=" * 70)
        print("\n📊 Target Distribution:")
        for tier, config in TIER_CONFIG.items():
            print(f"   {tier}: {config['target_count']} creators ({config['percentage']}%)")
        print(f"\n   Total Target: {sum(c['target_count'] for c in TIER_CONFIG.values())} creators")
        print("\n📁 Categories: ", list(CATEGORIES.keys()))
        print("=" * 70)
        
        all_creators = []
        
        for category_name, category_config in CATEGORIES.items():
            creators = self.find_creators_for_category(category_name, category_config)
            all_creators.extend(creators)
            print(f"\n📊 Running Total: {len(all_creators)} creators")
            print(f"   Tier Distribution: {self.tier_counts}")
        
        # Create DataFrame
        df = pd.DataFrame(all_creators)
        
        # Select and order columns for CSV output (matching creators_table.csv format)
        output_columns = [
            'Channel name', 'Link', 'Subscribers', 'Avg Views',
            'Content Category', 'Subscriber Category', 'Why this Creator'
        ]
        df_output = df[[col for col in output_columns if col in df.columns]]
        
        # Save to CSV
        df_output.to_csv(output_file, index=False)
        
        # Also save detailed version with all data
        detailed_file = output_file.replace('.csv', '_detailed.csv')
        df.to_csv(detailed_file, index=False)
        
        # Save to Excel with formatting
        excel_file = output_file.replace('.csv', '.xlsx')
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            df_output.to_excel(writer, index=False, sheet_name='All Creators')
            
            # Create separate sheets per category
            for category in CATEGORIES.keys():
                category_df = df_output[df_output['Content Category'] == category]
                sheet_name = category[:31]  # Excel sheet name limit
                category_df.to_excel(writer, index=False, sheet_name=sheet_name)
            
            # Create sheets per tier
            for tier in TIER_CONFIG.keys():
                tier_df = df_output[df_output['Subscriber Category'] == tier]
                sheet_name = tier.split('(')[0].strip()[:31]
                tier_df.to_excel(writer, index=False, sheet_name=sheet_name)
        
        print("\n" + "=" * 70)
        print("✅ COMPLETE!")
        print("=" * 70)
        print(f"\n📁 Output Files:")
        print(f"   CSV (simple): {output_file}")
        print(f"   CSV (detailed): {detailed_file}")
        print(f"   Excel: {excel_file}")
        print(f"\n📊 Total creators found: {len(df)}")
        
        print("\n📈 Breakdown by Category:")
        print(df['Content Category'].value_counts().to_string())
        
        print("\n📈 Breakdown by Tier:")
        print(df['Subscriber Category'].value_counts().to_string())
        
        return df


def main():
    if not YOUTUBE_API_KEY or YOUTUBE_API_KEY == "":
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
    output_file = f"higgsfield_creators_{timestamp}.csv"
    
    df = finder.run(output_file)
    
    print("\n🎯 Next steps:")
    print("1. Review the CSV and refine 'Why This Creator' column manually")
    print("2. Remove any irrelevant channels")
    print("3. Add any manually-found creators")
    print("4. Copy data to your Google Sheet")


if __name__ == "__main__":
    main()
