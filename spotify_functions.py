import base64
from requests import post,get
import json
import pandas as pd
import s3fs

def spotify_artist_search_and_save():
    
    # Replace these with your actual client ID, client secret, and artist name
    client_id = "a9a65070f7dd43408a9b1185f92c8966"
    client_secret = "cbf316d2e45f468680e1ca3f759813ec"
    artist_name = "arijit"
    
    def get_token():
        auth_string = client_id + ":" + client_secret
        auth_bytes = auth_string.encode("utf-8")
        auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")

        url = "https://accounts.spotify.com/api/token"
        headers = {
            "Authorization": "Basic " + auth_base64,
            "Content-Type": "application/x-www-form-urlencoded"
        }
        data = {"grant_type": "client_credentials"}
        result = post(url, headers=headers, data=data)
        json_result = json.loads(result.content)
        token = json_result["access_token"]
        return token

    def get_auth_header(token):
        return {"Authorization": "Bearer " + token}

    def search_for_artist(token, artist_name):
        url = "https://api.spotify.com/v1/search"
        headers = get_auth_header(token)
        query = f"?q={artist_name}&type=artist&limit=1"
        query_url = url + query
        result = get(query_url, headers=headers)
        json_result = json.loads(result.content)["artists"]["items"]
        
        if len(json_result) == 0:
            print("No artist with this name exists")
            return None
        return json_result[0]

    def get_song_by_artist(token, artist_id):
        url = f"https://api.spotify.com/v1/artists/{artist_id}/top-tracks?country=IN"
        headers = get_auth_header(token)
        result = get(url, headers=headers)
        json_result = json.loads(result.content)['tracks']
        return json_result

    token = get_token()
    result = search_for_artist(token, artist_name)
    artist_id = result['id']
    songs = get_song_by_artist(token, artist_id)

    ids = []
    song_name = []
    for idx, song in enumerate(songs):
        ids.append(idx + 1)
        song_name.append(song['name'])

    songs_data = pd.DataFrame({'id': ids, 'Top_Tracks': song_name})
    songs_data = songs_data.set_index('id')
    songs_data.to_csv(f"s3://airflow-spotify-bucket-new/result_{artist_name}.csv")
