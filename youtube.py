import os
import sys
import secrets
from datetime import datetime, timedelta
import pandas as pd
from google_auth_oauthlib.flow import InstalledAppFlow
import googleapiclient.discovery
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

# Global Usage
folder_path = r'C:\Users\research\OneDrive - Atlanta Convention & Visitors Bureau (1)\Shared Documents - ACVB Research'
today = datetime.now().replace(day=1) + timedelta(days=31) - timedelta(days=datetime.now().replace(day=1).day)
today = today.strftime("%Y-%m-%d")
log_dir = f'{folder_path}/log'
log_file_path = f'{log_dir}/Youtubelog_{today}.txt'
startd = '2020-01-01'
endd = today  # End date set to last day of current month
token_file = 'token.json'

# Set up logging to file
def log_print(*args, **kwargs):
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    with open(log_file_path, 'a', encoding='utf-8', errors='replace') as f:  # Replace problematic characters
        print(*args, file=f, **kwargs)
    print(*args, **kwargs)


def authenticate_youtube_api():
    client_secrets_file = 'C:/Users/research/Documents/MarketingMetrics/client_secret.json'
    scopes = ['https://www.googleapis.com/auth/youtube.readonly', 'https://www.googleapis.com/auth/yt-analytics.readonly']
    flow = InstalledAppFlow.from_client_secrets_file(client_secrets_file, scopes)
    credentials = None
    state = secrets.token_urlsafe()
    
    if os.path.exists(token_file):
        credentials = Credentials.from_authorized_user_file(token_file, scopes)
        log_print("Loaded credentials from token file")
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
            log_print("Refreshed expired credentials")
        else:
            for port in range(8080, 8090):
                try:
                    credentials = flow.run_local_server(port=port, state=state)
                    log_print(f"Authenticated on port {port} with state verification")
                    break
                except OSError:
                    log_print(f"Port {port} is unavailable")
    return credentials

def get_uploads_playlist_id(youtube, channel_id=None):
    if channel_id:
        request = youtube.channels().list(
            part="contentDetails",
            id=channel_id  # Uses the channel ID if provided
        )
    else:
        request = youtube.channels().list(
            part="contentDetails",
            mine=True  # Uses 'mine=True' if no channel ID is provided
        )
    response = request.execute()
    uploads_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    log_print(f"Retrieved Uploads Playlist ID: {uploads_playlist_id}")
    return uploads_playlist_id

def get_all_channel_videos(youtube, uploads_playlist_id):
    all_videos = []
    next_page_token = None
    page_count = 0
    while True:
        page_count += 1
        try:
            playlist_request = youtube.playlistItems().list(
                playlistId=uploads_playlist_id,
                part="snippet",
                maxResults=50,
                pageToken=next_page_token
            )
            playlist_response = playlist_request.execute()
            video_ids = [item['snippet']['resourceId']['videoId'] for item in playlist_response['items']]
            all_videos.extend(video_ids)
            log_print(f"Page {page_count}: Retrieved {len(video_ids)} videos, Total so far: {len(all_videos)}")
            next_page_token = playlist_response.get('nextPageToken')
            if not next_page_token:
                log_print("No more pages to fetch.")
                break
        except googleapiclient.errors.HttpError as e:
            log_print(f"API request failed with error: {e}")
            if e.resp.status in [403, 429]:
                log_print("We hit a quota or rate limit, stopping...")
                break
    return all_videos

def fetch_video_details(youtube, youtubeAnalytics, video_ids):
    videos_data = []
    log_print("Starting the YouTube data fetching process.")
    for video_id in video_ids:
        try:
            video_request = youtube.videos().list(part="snippet,statistics", id=video_id)
            video_response = video_request.execute()
            log_print("Video API Response", video_response)  # Debugging API response
            for item in video_response.get('items', []):
                title = item['snippet']['title']
                analytics_request = youtubeAnalytics.reports().query(
                    ids='channel==MINE',
                    startDate=startd,
                    endDate=endd,
                    dimensions='day,insightTrafficSourceType',
                    metrics='views,averageViewDuration',
                    sort='day',
                    filters=f'video=={video_id}'
                )
                analytics_response = analytics_request.execute()
                log_print("Analytics API Response", analytics_response)  # Debugging API response
                for row in analytics_response.get('rows', []):
                    videos_data.append({
                        'video_id': video_id,
                        'title': title,
                        'day': row[0],
                        'insightTrafficSourceType': row[1],
                        'view_count': row[2],
                        'average_view_duration': row[3],
                        'like_count': item['statistics'].get('likeCount'),
                        'comment_count': item['statistics'].get('commentCount')
                    })
        except googleapiclient.errors.HttpError as e:
            log_print(f"Failed to fetch data for video {video_id} due to: {e}")

    # Creating DataFrame and dropping duplicates
    df = pd.DataFrame(videos_data)
    df.drop_duplicates(subset=['video_id', 'day', 'insightTrafficSourceType'], inplace=True)
    return df

def main():
    try:
        credentials = authenticate_youtube_api()
        youtube = googleapiclient.discovery.build('youtube', 'v3', credentials=credentials)
        youtubeAnalytics = googleapiclient.discovery.build('youtubeAnalytics', 'v2', credentials=credentials)
        
        channel_id = 'UCrfdAtinUodsj_hcjgTJBVA'  # Set this to the actual channel ID or None if using mine=True
        
        uploads_playlist_id = get_uploads_playlist_id(youtube, channel_id)
        log_print("Starting video retrieval process...")
        
        video_ids = get_all_channel_videos(youtube, uploads_playlist_id)
        if video_ids:
            log_print(f"Fetching details for {len(video_ids)} videos...")
            video_df = fetch_video_details(youtube, youtubeAnalytics, video_ids)  # Ensure all three arguments are passed here
            xlsx_path = os.path.join(folder_path, 'youtube_data.xlsx')
            video_df.to_excel(xlsx_path, index=False)
            log_print(f"Dataframe saved successfully as {xlsx_path}.")
        else:
            log_print("No videos found.")
    except Exception as e:
        log_print(f"Failed during execution: {str(e)}")

if __name__ == '__main__':
    main()
