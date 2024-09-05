import json
from googleapiclient.discovery import build
import csv
import os
import boto3

API_KEY = os.getenv('API_KEY')

youtube = build(
    'youtube', 'v3', developerKey=API_KEY
)
bucket_name = "youtube-analytics-data"

s3_client = boto3.client('s3')


def write_to_csv(filename, data, category):
    """
    This fucntion is used to write the data as csv to S3 bucket
    """

    video_schema = ['video_id', 'channel_id', 'published_at', 'title', 'description', 'hashtags', 'audio_language', 'content_duration', 'virtual_dimension', 'quality_definition', 'privacy_status', 'topic_category', 'views_count', 'likes_count', 'dislikes_count', 'favorites_count', 'comments_count']
    channel_schema = ["channel_id", "title", "description", "published_at", "default_language", "country", "views_count", "subscribers_count", "videos_count"]
    
    with open(f"/tmp/{filename}", 'w', newline='', encoding='utf-8-sig') as csvfile:
        # creating a csv dict writer object
        csvwriter = csv.writer(csvfile)

        if category == "video":
            # writing the fields
            csvwriter.writerow(video_schema)
        else:
            csvwriter.writerow(channel_schema)

        # writing the data rows
        csvwriter.writerows(data[:])
        
    
    response = s3_client.upload_file(f"/tmp/{filename}", bucket_name, f'youtube_handle_data/{category}/{filename}')

    print(f'upload_log_to_aws response: {response}')
    
    
    # csv_buffer = io.StringIO()
    
    # csvwriter = csv.writer(csv_buffer)

    # if category == "video":
    #     # writing the fields
    #     csvwriter.writerow(video_schema)
    # else:
    #     csvwriter.writerow(channel_schema)

    # # writing the data rows
    # csvwriter.writerows(data[:])
    
    # s3_resource.Object("youtube-analytics-data", f'youtube_handle_data/{category}/{filename}').put(Body=csv_buffer.getvalue())
       
    print(f"successfully written file - s3://youtube-analytics-data/youtube_handle_data/{category}/{filename}")


def get_channels(channel_name_lst):
    """
    This function helps to get the channel info based on the channel names we pass it as param and store it as dict format.
    """

    channel_data_info = {}
    channel_id_info = []
    for channel_name in channel_name_lst:
        print(f"{channel_name=}")
        channel_data = []
        request = youtube.channels().list(
            part='contentDetails,snippet,statistics', forHandle=f"@{channel_name}"
        )
        channel_resp = request.execute()

        if channel_resp.get('items'):
            ch_data = channel_resp.get('items')[0]
            channel_data.append(ch_data.get("id"))
            channel_data.append(ch_data.get("snippet")["title"])
            channel_data.append(ch_data.get("snippet")["description"])
            channel_data.append(ch_data.get("snippet")["publishedAt"])
            channel_data.append(ch_data.get("snippet").get('defaultLanguage', 'en'))
            channel_data.append(ch_data.get("snippet").get('country', 'global'))
            channel_data.append(ch_data.get("statistics")['viewCount'])
            channel_data.append(ch_data.get("statistics")['subscriberCount'])
            channel_data.append(ch_data.get("statistics")['videoCount'])
            channel_id_info.append(ch_data.get("id"))
        channel_data_info[channel_name] = channel_data

    return channel_data_info, channel_id_info

def get_videos(channel_id):
    """
    This fucntion helps to hit the youtube API and get the channel info store as list
    """
    
    videos = []
    next_page_token = None
    while True:
        if next_page_token:
            request = youtube.search().list(
                part='Id', channelId=channel_id, maxResults=50, order="date", type="video", pageToken=next_page_token
            )
        else:
            request = youtube.search().list(
                part='Id', channelId=channel_id, maxResults=50, order="date", type="video"
            )
        videos_resp = request.execute()
        videos.extend(videos_resp.get('items', []))
        
        next_page_token = videos_resp.get('nextPageToken')
        if not next_page_token:
            break
        # url = f"{url}&pageToken={next_page_token}"
    return {video['id']['videoId'] for video in videos}

def get_video_statistics(video_ids):
    """
    This fucntion helps to hit the youtube API and get the video stats info store as list
    """

    video_data = []
    
    video_ids = list(video_ids)

    for i in range(0, len(video_ids), 50):
        video_ids_chunk = video_ids[i:i + 50]
        # url = f"https://www.googleapis.com/youtube/v3/videos?part=statistics&id={','.join(video_ids_chunk)}&key={API_KEY}"
        request = youtube.videos().list(
            part='contentDetails,id,liveStreamingDetails,localizations,player,recordingDetails,snippet,statistics,status,topicDetails', id=','.join(video_ids_chunk)
        )
        response = request.execute()
        
        video_data.extend(response.get('items', []))
    return video_data

def parse_video_data(video_data):
    """
    This function helps to parse the video data based on the list of info extracted from the youtube api and convert the relevant info as list.
    """

    video_lst = []
    for data_chunk in video_data:
        video_info = []
        print("video_id is ", data_chunk.get('id'))
        video_info.append(data_chunk.get('id'))
        video_info.append(data_chunk.get('snippet')['channelId'])
        video_info.append(data_chunk.get('snippet')['publishedAt'])
        video_info.append(data_chunk.get('snippet')['title'])
        video_info.append(data_chunk.get('snippet')['description'])
        video_info.append(",".join(data_chunk.get('snippet').get('tags', [])))
        video_info.append(data_chunk.get('snippet')['localized'].get('defaultAudioLanguage', 'en'))
        video_info.append(data_chunk.get('contentDetails')['duration'])
        video_info.append(data_chunk.get('contentDetails')['dimension'])
        video_info.append(data_chunk.get('contentDetails')['definition'])
        video_info.append(data_chunk.get('status')['privacyStatus'])
        topic_cat = data_chunk.get('topicDetails',{'topicCategories': []})['topicCategories']
        video_info.append(",".join([topic.rsplit('/')[-1] for topic in topic_cat]))
        video_info.append(data_chunk.get('statistics')['viewCount'])
        video_info.append(data_chunk.get('statistics')['likeCount'])
        video_info.append(data_chunk.get('statistics').get('dislikeCount', 0))
        video_info.append(data_chunk.get('statistics')['favoriteCount'])
        video_info.append(data_chunk.get('statistics').get('commentCount',0))

        video_lst.append(video_info)
    return video_lst


def channel_videos_info(channel_ids):
    """
    This function helps to drive the info related to videos extraction.
    """
    final_videos_lst = []
    for channel_id in channel_ids:
        videos_id_lst = get_videos(channel_id)
        videos_data = get_video_statistics(videos_id_lst)
        videos_result_lst = parse_video_data(videos_data)
        final_videos_lst.extend(videos_result_lst)
    return final_videos_lst




def lambda_handler(event, context):
    
    print(f"{event=}")
    
    try:
    
        channel_names = ['straitstimesonline', 'TheBusinessTimes', 'zaobaodotsg','Tamil_Murasu', 'BeritaHarianSG1957']
    
        print('Extracting channel_info')
    
        channel_data_lst, channel_ids = get_channels(channel_names)
        channel_data_lst, channel_ids
    
        print("saved channel info to S3 bucket")
    
        write_to_csv("youtube_channels_data.csv", list(channel_data_lst.values()), "channel")
        
        print('Extracting video info')
        
        videos_detail_info = channel_videos_info(channel_ids)
        
        write_to_csv("youtube_videos_data.csv", videos_detail_info, "video")
    
        print("saved video info to S3 bucket")
        
    except Exception as e:
        print("Error is", e)
        return {
            'statusCode': 500,
            'body': json.dumps('Error occurred -', e)
        }
        
    
    return {
        'statusCode': 200,
        'body': json.dumps('successfully written the output to S3')
    }
