

from dotenv import load_dotenv
import os
import pandas as pd
import requests
from google.cloud import bigquery
from prefect import flow, task
from datetime import datetime, timedelta

load_dotenv()

LASTFM_API_KEY = os.getenv("LASTFM_API_KEY")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS

BQ_TABLE_GEO_ARTISTS = "lastfm_geo_top_artists"
BQ_TABLE_GEO_TRACKS = "lastfm_geo_top_tracks"
BQ_TABLE_CHART_ARTISTS = "lastfm_chart_top_artists"
BQ_TABLE_CHART_TRACKS = "lastfm_chart_top_tracks"
BQ_TABLE_CHART_TAGS = "lastfm_chart_top_tags"

BASE_URL = "https://ws.audioscrobbler.com/2.0/"


@task
def extract_global(method: str, **params):
    print(f" Mengambil data dari {method}...")
    base_params = {
        "method": method,
        "api_key": LASTFM_API_KEY,
        "format": "json",
        "limit": 100
    }
    base_params.update(params)

    response = requests.get(BASE_URL, params=base_params)
    response.raise_for_status()
    data = response.json()

    key_map = {
        "geo.gettopartists": ("topartists", "artist"),
        "geo.gettoptracks": ("tracks", "track"),
        "chart.gettopartists": ("artists", "artist"),
        "chart.gettoptracks": ("tracks", "track"),
        "chart.gettoptags": ("tags", "tag"),
    }

    parent_key, child_key = key_map[method]
    records = data.get(parent_key, {}).get(child_key, [])

    df = pd.DataFrame(records)
    if df.empty:
        print(f" Tidak ada data untuk {method}")
    else:
        print(f" Berhasil ambil {len(df)} baris data dari {method}")
    return df

@task
def transform_global(df, level: str):
    if df.empty:
        print(f" Tidak ada data {level}")
        return df

    print(f" Transformasi data top {level}...")
    now = datetime.utcnow().isoformat()

    if level in ["geo_artists", "chart_artists"]:
        df["artist_name"] = df["name"]
        df["listeners"] = df["listeners"].astype(int)
        df["playcount"] = df.get("playcount", None)
        df["timestamp"] = now
        df = df[["artist_name", "listeners", "playcount", "timestamp"]]

    elif level in ["geo_tracks", "chart_tracks"]:
        df["artist_name"] = df["artist"].apply(lambda x: x.get("name") if isinstance(x, dict) else None)
        df["track_name"] = df["name"]
        df["listeners"] = df["listeners"].astype(int)
        df["timestamp"] = now
        df = df[["artist_name", "track_name", "listeners", "timestamp"]]

    elif level == "chart_tags":
        df["tag_name"] = df["name"]
        df["reach"] = df["reach"].astype(int)
        df["taggings"] = df["taggings"].astype(int)
        df["timestamp"] = now
        df = df[["tag_name", "reach", "taggings", "timestamp"]]

    return df

@task
def load_to_bigquery(df, table_name):
    if df.empty:
        print(f"Data kosong, tidak diupload ke {table_name}")
        return

    client = bigquery.Client(project=GCP_PROJECT_ID)
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}"
    print(f" Upload {len(df)} baris ke {table_id}...")

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    table = client.get_table(table_id)
    print(f" Upload selesai. Total rows di tabel: {table.num_rows}")


@flow(name="LastFM_Global_TopData_to_BigQuery")
def etl_global_topdata():
    print(f" Menjalankan ETL Global pada {datetime.now()}")

    df_geo_artists = extract_global("geo.gettopartists", country="indonesia")
    df_geo_artists_t = transform_global(df_geo_artists, "geo_artists")
    load_to_bigquery(df_geo_artists_t, BQ_TABLE_GEO_ARTISTS)

    df_geo_tracks = extract_global("geo.gettoptracks", country="indonesia")
    df_geo_tracks_t = transform_global(df_geo_tracks, "geo_tracks")
    load_to_bigquery(df_geo_tracks_t, BQ_TABLE_GEO_TRACKS)

    df_chart_artists = extract_global("chart.gettopartists")
    df_chart_artists_t = transform_global(df_chart_artists, "chart_artists")
    load_to_bigquery(df_chart_artists_t, BQ_TABLE_CHART_ARTISTS)

    df_chart_tracks = extract_global("chart.gettoptracks")
    df_chart_tracks_t = transform_global(df_chart_tracks, "chart_tracks")
    load_to_bigquery(df_chart_tracks_t, BQ_TABLE_CHART_TRACKS)

    df_chart_tags = extract_global("chart.gettoptags")
    df_chart_tags_t = transform_global(df_chart_tags, "chart_tags")
    load_to_bigquery(df_chart_tags_t, BQ_TABLE_CHART_TAGS)

if __name__ == "__main__":
    from datetime import timedelta
    etl_global_topdata.serve(
        name="lastfm-global-etl-daily",
        interval=timedelta(days=1)
    )


