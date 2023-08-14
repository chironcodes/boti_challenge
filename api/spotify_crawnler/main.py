from dotenv import load_dotenv
import polars as pl
from os import getenv
import requests


load_dotenv('.env')


CLIENT_ID = getenv('CLIENT_ID')
CLIENT_SECRET = getenv('CLIENT_SECRET')


def get_spotify_bearer_token(client_id:str, client_secret:str) -> str:
    try:
        url = "https://accounts.spotify.com/api/token"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        data = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret
        }

        response = requests.post(url, headers=headers, data=data)
        response_data = response.json()

    
        return response_data["access_token"]
    
    except Exception as err:
        print(f"err {err=}")

def get_spotify_spotify_episodes(query:str, token, market='BR', limit=50) -> str:
    try:
        url = f"https://api.spotify.com/v1/search?q={query}&type=episode&market=BR&limit=50&include_external=audio"
        headers = {
            'Authorization': f'Bearer {token}'
        }
        response = requests.get(url, headers=headers)
        data = response.json()

        return data
    
    except Exception as err:
        print(f"err {err=}")



if __name__ == '__main__':


    token = get_spotify_bearer_token(CLIENT_ID, CLIENT_SECRET)

    results = get_spotify_spotify_episodes('data hackers', token)

    df = pl.DataFrame(results.get("episodes").get("items"))


    print(df.limit(1))