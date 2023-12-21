from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime
import pandas as pd
import time
import os
import json
import csv
import requests
import re

YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'
DEVELOPER_KEYS = [
    'AIzaSyARTyZ_96Z2kz8PeC8U2fuLcwDoFxSr1Pw',
    'AIzaSyBBkeI6IJrkgDKLgITt5pRynWMt2XWYeTQ',
    'AIzaSyDAggdaCkwlAxcbC9m-wWu_9q6_tgqxPxU',
    'AIzaSyAyjpJtQ3BTXTOMAKUN0dgj96D9agtXP8c',
    'AIzaSyAoL1BGmJ-NKg9nwYxiCIQyobt3lUWRMHQ',
    'AIzaSyCU9XNRsACGNBQjv7rEKPUI-UmpiobAZbQ',
    'AIzaSyAk3VAZWvyIyRNA2isXjI4ndJuLBKxqaEA',
    'AIzaSyDYRnkJgc4bQmX9iR3Jm7Zzls9r5hRkjeg',
    'AIzaSyBZ4jpbZazAQ5_nUhstH_sLKkFkFjcAW_M',
    'AIzaSyC-XcY-yQyhHFAGe-YWtM8TFG9mgfF6shU',
    'AIzaSyD47qwsIvhbbYnp4gXXDdaLGDPFKWxFM_Q',
    'AIzaSyCGiBC0JShpL44J7053XANXvfr2oaR6Ka8',
    'AIzaSyAEAnNCTka5Qq9xON8qboEyLkruNIVZDm8',
    'AIzaSyBzdn6lTJ21DJqRwhetFh0UyOav7gfLcP0',
    'AIzaSyA8VR0paydU1V8p2J7I6pFhyAUO9zURHSM',
    'AIzaSyDogewSEXt7Qufhges8xmXhT63IjkmIKj0',
    'AIzaSyDAXiE8UwkdYUPJuMD5yZqDA8vECiTVPP8',
    'AIzaSyBSCqkc1A0kuOhCT1sn1dI43p6t_xMJO9I',
    'AIzaSyA8VR0paydU1V8p2J7I6pFhyAUO9zURHSM',
]

CHANNELS_IDS = [
    'UCA0aqdkBHj5G4eS0raaeZhg',  # Padre Kelmon
    'UCHFO37KCJlMNUXNK21MV8SQ',  # Ciro Gomes
    'UCoqbOJs9BBUDKZWNz_BNYUA',  # Vera
    'UCvO2BExvkAbGMsTGnEnI_Ng',  # Lula
    'UC8hGUtfEgvvnp6IaHSAg1OQ',  # Jair Bolsonaro
    'UCIUrDYfCO0lMmuseV-wbRZQ',  # Simone Tebet
    'UCU0A7TGU0Evi_nDusL05hig',  # Soraya Thronicke
    'UC2wHoeqJJ2J-ZXYBgDM3dmw',  # Felipe D'avila
]

REQUIRED_TITLE_KEYWORDS = []

channel_id_index = 0

developer_keys = DEVELOPER_KEYS

config = {}

request_count = 0


def switch_key():
    global request_count, developer_keys

    if len(developer_keys) == 0:
        print(f"Iniciando estado de hibernação de 24 horas e 15 minutos")
        time_sleep = (24 * 60 * 60) + (15 * 60)
        time.sleep(time_sleep)
        developer_keys = DEVELOPER_KEYS

    new_key = developer_keys.pop()
    print(f"Chave de API trocada para: {new_key}")
    request_count = 0
    return build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=new_key)


youtube = switch_key()


def log_information(message):
    now_date = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    with open('log.txt', 'a') as log_file:
        log_file.write(f'{now_date} {message}\n')


def get_config():
    global config

    if not os.path.isfile("config.json"):
        config = {
            'channel_id_index': 0
        }
        return

    with open('config.json', 'r') as config_file:
        config = json.load(config_file)


def save_config():
    global config
    with open('config.json', 'w') as config_file:
        json.dump(config, config_file)


def is_short_video(video_id):
    shorts_url = f"https://www.youtube.com/shorts/{video_id}"
    response = requests.head(shorts_url, allow_redirects=False)

    return response.status_code == 200


def get_video_details(video_id):
    global youtube
    video_response = youtube.videos().list(
        part="snippet,statistics,contentDetails,status",
        id=video_id
    ).execute()

    video_details = video_response.get('items', [])[0]
    snippet = video_details['snippet']
    is_short = is_short_video(video_id)

    return {
        "video_id": video_id,
        "title": snippet['title'],
        "is_short": is_short,
        "description": snippet['description'],
        "channel_id": snippet['channelId'],
        "published_at": snippet['publishedAt'],
        "category_id": snippet.get('categoryId'),
        "tags": snippet.get('tags', []),
        "view_count": int(video_details['statistics'].get('viewCount', 0)),
        "like_count": int(video_details['statistics'].get('likeCount', 0)),
        "comment_count": int(video_details['statistics'].get('commentCount', 0)),
        "duration": video_details['contentDetails'].get('duration'),
        "privacy_status": video_details['status'].get('privacyStatus'),
        "license": video_details['status'].get('license'),
        "is_made_for_kids": video_details['status'].get('madeForKids'),
        "thumbnail_url": snippet['thumbnails'].get('high', {}).get('url'),
        "default_audio_language": snippet.get('defaultAudioLanguage'),
        "default_language": snippet.get('defaultLanguage'),
        "country": snippet['localized'].get('country') if 'localized' in snippet else None
    }


def get_channel_details(channel_id):
    global youtube
    channel_response = youtube.channels().list(
        part="snippet,statistics,contentDetails",
        id=channel_id
    ).execute()

    channel_details = channel_response.get('items', [])[0]
    return {
        "channel_id": channel_id,
        "title": channel_details['snippet']['title'],
        "description": channel_details['snippet'].get('description'),
        "published_at": channel_details['snippet']['publishedAt'],
        "view_count": channel_details['statistics'].get('viewCount'),
        "comment_count": channel_details['statistics'].get('commentCount'),
        "subscriber_count": channel_details['statistics'].get('subscriberCount'),
        "video_count": channel_details['statistics'].get('videoCount'),
        "is_verified": 'brandingSettings' in channel_details and 'channel' in channel_details[
            'brandingSettings'] and 'isVerified' in channel_details['brandingSettings']['channel']
    }


def get_replies(video_id, comment_id):
    global request_count, youtube
    replies_data = []
    page_token = None

    while True:
        try:
            replies_response = youtube.comments().list(
                part="snippet",
                parentId=comment_id,
                maxResults=100,
                pageToken=page_token,
                textFormat="plainText"
            ).execute()

            request_count += 1
            if request_count >= 1000:
                youtube = switch_key()
                request_count = 0

            for item in replies_response.get('items', []):
                reply_info = item["snippet"]
                replies_data.append({
                    "video_id": video_id,
                    "comment_id": item["id"],
                    "author": reply_info.get("authorDisplayName"),
                    "comment": reply_info.get("textDisplay"),
                    "published_at": reply_info.get("publishedAt"),
                    "like_count": reply_info.get("likeCount"),
                    "is_reply": True,
                    "parent_id": comment_id
                })

            page_token = replies_response.get('nextPageToken')
            if not page_token:
                break
        except HttpError as e:
            handle_http_error(e)
            break

    return replies_data


def handle_http_error(e):
    global developer_keys, youtube

    if e.resp.status == 403:
        error_reason = e._get_reason()
        if "quota" in error_reason.lower():
            print("Quota excedida. A execução não pode continuar até que a quota seja renovada.")
            if len(developer_keys) == 0:
                time_to_wait = 24 * 60 * 60 + 15 * 60
                time.sleep(time_to_wait)
                developer_keys = DEVELOPER_KEYS
            youtube = switch_key()
        else:
            print(f"Acesso negado. O erro pode ser devido a comentários desativados ou questões de quota.")
    elif e.resp.status == 404:
        print("Comentário ou vídeo não encontrado.")
    else:
        print(f"Ocorreu um erro HTTP {e.resp.status}: {e.content}")


def get_comments(video_id, video_title):
    global request_count, youtube
    comments_data = []
    page_token = None
    total_collected = 0

    video_details = youtube.videos().list(
        part="statistics",
        id=video_id
    ).execute()

    total_comments = video_details.get('items', [])[0]['statistics'].get('commentCount', 0)
    print(f"Coletando comentários do vídeo: {video_title}")

    while True:
        try:
            response = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=100,
                pageToken=page_token,
                textFormat="plainText"
            ).execute()

            request_count += 1
            if request_count >= 1000:
                youtube = switch_key()
                request_count = 0

            items = response.get("items", [])
            total_collected += len(items)

            for item in items:
                comment_info = item["snippet"]["topLevelComment"]["snippet"]
                comment_id = item["snippet"]["topLevelComment"]["id"]
                comments_data.append({
                    "video_id": video_id,
                    "comment_id": comment_id,
                    "author": comment_info.get("authorDisplayName"),
                    "comment": comment_info.get("textDisplay"),
                    "published_at": comment_info.get("publishedAt"),
                    "like_count": comment_info.get("likeCount"),
                    "is_reply": False,
                    "parent_id": None
                })

                replies = get_replies(video_id, comment_id)
                comments_data.extend(replies)

            page_token = response.get('nextPageToken')
            if not page_token:
                print(f"{total_collected} comentários do total de {total_comments} coletados do vídeo ID: {video_id}")
                break
        except HttpError as e:
            handle_http_error(e)
            break
    return comments_data


def process_video(video_id, video_title, processed_videos):
    global youtube, request_count

    videos_file_exists = os.path.isfile('videos_info.csv')
    channels_file_exists = os.path.isfile('channels_info.csv')
    comments_file_exists = os.path.isfile('comments_info.csv')

    video_details = get_video_details(video_id)
    if video_details['comment_count'] > 0:
        pd.DataFrame([video_details]).to_csv('video_info.csv', mode='a', header=not videos_file_exists, index=False)

        channel_details = get_channel_details(video_details['channel_id'])
        pd.DataFrame([channel_details]).to_csv('channels_info.csv', mode='a', header=not channels_file_exists,
                                               index=False)

        comments = get_comments(video_id, video_title)
        comments_df = pd.DataFrame(comments)
        comments_df['channel_id'] = video_details['channel_id']
        comments_df.to_csv('comments_info.csv', mode='a', header=not comments_file_exists, index=False)

    processed_videos.add(video_id)
    with open('processed_videos.csv', 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([video_id])


def main():
    global youtube, config, channel_id_index, request_count
    start_date = datetime(2022, 8, 16).isoformat() + 'Z'
    end_date = datetime(2022, 11, 6).isoformat() + 'Z'

    video_details_list = []

    get_config()

    channel_id_index = config['channel_id_index']

    try:
        with open('processed_videos.csv', 'r') as file:
            processed_videos = {row[0] for row in csv.reader(file)}
    except FileNotFoundError:
        processed_videos = set()

    while True:
        if channel_id_index > len(CHANNELS_IDS):
            break

        channel_id = CHANNELS_IDS[channel_id_index]
        print(f"Iniciando busca para o canal: {channel_id}")

        page_token = None
        while True:
            try:

                response = youtube.search().list(
                    part='snippet',
                    channelId=channel_id,
                    type='video',
                    publishedAfter=start_date,
                    publishedBefore=end_date,
                    maxResults=50,
                    pageToken=page_token
                ).execute()
                request_count += 1

                for item in response.get('items', []):
                    video_id = item['id']['videoId']
                    if video_id not in processed_videos:
                        video_details = get_video_details(video_id)
                        print("Title:", video_details['title'], "# comments", video_details['comment_count'])
                        # title_lower = video_details['title'].lower()
                        # if any(re.search(r'\b' + re.escape(term.lower()) + r'\b', title_lower) for term in REQUIRED_TITLE_KEYWORDS):
                        video_details_list.append(video_details)

                page_token = response.get('nextPageToken')

                if not page_token:
                    break
            except HttpError as e:
                print(f"Erro na requisição: {e}")
                if e.resp.status == 403 and "quotaExceeded" in e.content.decode():
                    print("Cota excedida. Tentando com uma nova chave de API.")
                    youtube = switch_key()
                else:
                    break
        for video in video_details_list:
            process_video(video['video_id'], video['title'], processed_videos)
            log_information(f"Processando video: {video['video_id']}")

        channel_id_index += 1
        config['channel_id_index'] = channel_id_index
        save_config()


if __name__ == "__main__":
    main()
