#!/usr/bin/env python3
"""
NPTEL Course Downloader - Simplified Version
Downloads complete NPTEL course playlists from YouTube
"""

import os
import re
import sys
import time
import yt_dlp

class NPTELDownloader:
    def __init__(self):
        self.base_download_path = "NPTEL_Courses"
        
    def sanitize_filename(self, filename: str) -> str:
        """Remove invalid characters from filename"""
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '')
        return filename.strip()[:200]
    
    def extract_playlist_info(self, url: str):
        """Extract basic playlist information"""
        print("\n📋 Fetching playlist information...")
        
        ydl_opts = {
            'quiet': True,
            'extract_flat': True,
            'force_generic_extractor': False,
        }
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            try:
                info = ydl.extract_info(url, download=False)
                playlist_title = info.get('title', 'NPTEL_Course')
                total_videos = len(info.get('entries', []))
                uploader = info.get('uploader', 'Unknown')
                
                print(f"✅ Playlist: {playlist_title}")
                print(f"📺 Channel: {uploader}")
                print(f"📊 Total Videos: {total_videos}")
                
                return playlist_title, total_videos
                
            except Exception as e:
                print(f"❌ Error accessing playlist: {e}")
                return None, 0
    
    def get_all_video_urls(self, playlist_url: str):
        """Get all video URLs from the playlist"""
        print("\n🔍 Fetching all video links from playlist...")
        print("⏳ This may take a moment for large playlists...")
        
        ydl_opts = {
            'quiet': True,
            'extract_flat': False,  # Get full info for each video
            'ignoreerrors': True,   # Continue even if some videos are unavailable
        }
        
        videos = []
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            try:
                print("   Starting to fetch video information...")
                playlist_info = ydl.extract_info(playlist_url, download=False)
                
                if 'entries' not in playlist_info:
                    print("❌ No videos found in playlist!")
                    return []
                
                entries = playlist_info['entries']
                total = len(entries)
                
                print(f"   Found {total} videos. Processing...")
                
                for i, entry in enumerate(entries, 1):
                    # Show progress every 5 videos
                    if i % 5 == 0 or i == 1 or i == total:
                        print(f"   📊 Processing video {i}/{total}... ({i*100//total}%)")
                    
                    if entry:  # Check if entry is not None
                        video_info = {
                            'title': entry.get('title', f'Video_{i}'),
                            'url': entry.get('webpage_url', entry.get('url', '')),
                            'duration': entry.get('duration', 0),
                            'index': i
                        }
                        videos.append(video_info)
                    else:
                        print(f"   ⚠️  Video {i} is unavailable or private")
                        # Still add a placeholder to maintain numbering
                        videos.append({
                            'title': f'Unavailable_Video_{i}',
                            'url': None,
                            'duration': 0,
                            'index': i
                        })
                
                print(f"\n✅ Successfully fetched {len([v for v in videos if v['url']])} available videos")
                
                return videos
                
            except Exception as e:
                print(f"❌ Error fetching playlist: {e}")
                return []
    
    def download_video(self, video_info: dict, output_path: str) -> bool:
        """Download a single video"""
        if not video_info['url']:
            print(f"   ⚠️  Skipping unavailable video: {video_info['title']}")
            return False
        
        # Create filename with lecture number
        lecture_num = video_info['index']
        clean_title = self.sanitize_filename(video_info['title'])
        filename = f"Lecture_{lecture_num:03d} - {clean_title}"
        
        ydl_opts = {
            'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
            'merge_output_format': 'mp4',
            'outtmpl': os.path.join(output_path, f'{filename}.%(ext)s'),
            'quiet': True,
            'no_warnings': True,
            'progress_hooks': [lambda d: self.progress_hook(d, lecture_num)],
        }
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([video_info['url']])
            return True
        except Exception as e:
            print(f"\n   ❌ Error downloading: {e}")
            return False
    
    def progress_hook(self, d, lecture_num):
        """Display download progress"""
        if d['status'] == 'downloading':
            percent = d.get('_percent_str', 'N/A').strip()
            speed = d.get('_speed_str', 'N/A').strip()
            eta = d.get('eta', 0)
            
            # Convert ETA to readable format
            if eta and eta > 0:
                mins, secs = divmod(eta, 60)
                eta_str = f"{int(mins)}m {int(secs)}s"
            else:
                eta_str = "calculating..."
            
            print(f"\r   📥 Lecture {lecture_num:03d}: {percent} | Speed: {speed} | ETA: {eta_str}        ", end='')
            
        elif d['status'] == 'finished':
            print(f"\r   ✅ Lecture {lecture_num:03d}: Download complete, merging audio/video...        ")
    
    def download_playlist(self, playlist_url: str):
        """Download entire playlist"""
        # Get playlist info
        playlist_title, total_videos = self.extract_playlist_info(playlist_url)
        
        if not playlist_title:
            print("❌ Could not access playlist. Please check the URL.")
            return
        
        if total_videos == 0:
            print("❌ No videos found in the playlist!")
            return
        
        # Create course folder
        course_folder = self.sanitize_filename(playlist_title)
        course_path = os.path.join(self.base_download_path, course_folder)
        os.makedirs(course_path, exist_ok=True)
        
        print(f"\n📁 Download folder: {course_path}")
        
        # Get all video URLs
        videos = self.get_all_video_urls(playlist_url)
        
        if not videos:
            print("❌ Could not fetch video information!")
            return
        
        # Confirm download
        available_videos = [v for v in videos if v['url']]
        print(f"\n📊 Ready to download {len(available_videos)} videos")
        
        confirm = input("⚠️  Start downloading? (y/n): ").lower()
        if confirm != 'y':
            print("❌ Download cancelled")
            return
        
        # Download all videos
        print(f"\n🚀 Starting download of {len(available_videos)} videos...")
        print("=" * 60)
        
        downloaded = 0
        failed = 0
        skipped = 0
        
        for i, video in enumerate(videos, 1):
            print(f"\n📹 [{i}/{len(videos)}] {video['title'][:60]}...")
            
            if not video['url']:
                skipped += 1
                print("   ⚠️  Video unavailable, skipping...")
                continue
            
            success = self.download_video(video, course_path)
            
            if success:
                downloaded += 1
            else:
                failed += 1
            
            # Show progress summary
            print(f"   Progress: ✅ {downloaded} downloaded | ❌ {failed} failed | ⚠️  {skipped} skipped")
        
        # Final summary
        print("\n" + "=" * 60)
        print("📊 DOWNLOAD COMPLETE!")
        print(f"✅ Successfully downloaded: {downloaded}/{len(videos)} videos")
        if failed > 0:
            print(f"❌ Failed downloads: {failed}")
        if skipped > 0:
            print(f"⚠️  Skipped (unavailable): {skipped}")
        print(f"📁 All videos saved to: {course_path}")
        print("=" * 60)
    
    def run(self):
        """Main execution"""
        print("=" * 60)
        print("🎓 NPTEL PLAYLIST DOWNLOADER - Simple Version")
        print("=" * 60)
        
        print("\n📌 Instructions:")
        print("1. Go to your NPTEL course playlist on YouTube")
        print("2. Copy the playlist URL from your browser")
        print("3. Paste it here")
        
        while True:
            playlist_url = input("\n🔗 Paste playlist URL (or 'q' to quit): ").strip()
            
            if playlist_url.lower() == 'q':
                print("👋 Goodbye!")
                break
            
            if not playlist_url:
                print("❌ URL cannot be empty!")
                continue
            
            # Basic URL validation
            if 'youtube.com' not in playlist_url and 'youtu.be' not in playlist_url:
                print("❌ Please provide a valid YouTube playlist URL")
                print("   Example: https://www.youtube.com/playlist?list=...")
                continue
            
            # Download the playlist
            self.download_playlist(playlist_url)
            
            # Ask if user wants to download another
            another = input("\n🔄 Download another playlist? (y/n): ").lower()
            if another != 'y':
                print("👋 Goodbye!")
                break

def main():
    """Main entry point"""
    # Check for required package
    try:
        import yt_dlp
    except ImportError:
        print("❌ Required package 'yt-dlp' not found!")
        print("📦 Install it using: pip install yt-dlp")
        print("\nRun this command:")
        print("   pip install yt-dlp")
        sys.exit(1)
    
    # Create and run downloader
    downloader = NPTELDownloader()
    
    try:
        downloader.run()
    except KeyboardInterrupt:
        print("\n\n⚠️  Download interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()