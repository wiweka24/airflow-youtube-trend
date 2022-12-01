import requests
import sys, time, os
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

class extractYtApiOperator(BaseOperator):
  
  @apply_defaults
  def __init__(self, *args, **kwargs):
   super(extractYtApiOperator, self).__init__(*args, **kwargs)
  
  def execute(self, context):
    snippet_features = ["title",
                        "publishedAt",
                        "channelId",
                        "channelTitle",
                        "categoryId"]
    unsafe_characters = ['\n', '"']
    header = ["video_id"] + snippet_features + ["trending_date", "tags", "view_count", "likes", "comment_count"] 
                                                # "thumbnail_link", "description"

    def prepare_feature(feature):
        for ch in unsafe_characters:
            feature = str(feature).replace(ch, "")
        return f'"{feature}"'

    def api_request(page_token, country_code):
        request_url = f"https://www.googleapis.com/youtube/v3/videos?part=id,statistics,snippet{page_token}chart=mostPopular&regionCode={country_code}&maxResults=50&key={api_key}"
        request = requests.get(request_url)
        if request.status_code == 429:
            print("Temp-Banned due to excess requests, please wait and continue later")
            sys.exit()
        return request.json()

    def get_tags(tags_list):
        return prepare_feature("|".join(tags_list))

    def get_videos(items):
        lines = []
        for video in items:
            if "statistics" not in video:
                continue
            video_id = prepare_feature(video['id'])
            snippet = video['snippet']
            statistics = video['statistics']
            features = [prepare_feature(snippet.get(feature, "")) for feature in snippet_features]
            description = snippet.get("description", "")
            thumbnail_link = snippet.get("thumbnails", dict()).get("default", dict()).get("url", "")
            trending_date = time.strftime("%y.%d.%m")
            tags = get_tags(snippet.get("tags", ["[none]"]))
            view_count = statistics.get("viewCount", 0)
            
            if 'likeCount' in statistics:
                likes = statistics['likeCount']
                # dislikes = statistics['dislikeCount']
            else:
                likes = 0
            if 'commentCount' in statistics:
                comment_count = statistics['commentCount']
            else:
                comment_count = 0
            line = [video_id] + features + [prepare_feature(x) for x in [trending_date, tags, view_count, likes, comment_count]]
            lines.append(",".join(line))
        return lines

    def get_pages(country_code, next_page_token="&"):
        country_data = []
        while next_page_token is not None:
            video_data_page = api_request(next_page_token, country_code)
            
            if video_data_page.get('error'):
                print(video_data_page['error'])
            next_page_token = video_data_page.get("nextPageToken", None)
            next_page_token = f"&pageToken={next_page_token}&" if next_page_token is not None else next_page_token
            items = video_data_page.get('items', [])
            country_data += get_videos(items)
        return country_data

    def write_to_file(country_code, country_data):
        print(f"Writing {country_code} data to file...")
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        with open(f"{output_dir}/{country_code}_videos.csv", "w+", encoding='utf-8') as file:
            for row in country_data:
                file.write(f"{row}\n")

    def get_data():
        for country_code in country_codes:
            country_data = [",".join(header)] + get_pages(country_code)
            write_to_file(country_code, country_data)

    output_dir = "/opt/airflow/data/api"
    api_key = "AIzaSyAyC91r6h-lGuNp6hq8mhcO0fpkx4LrO-k"
    country_codes = ["ID", "MY", "SG", "EG", "GB"]
    get_data()