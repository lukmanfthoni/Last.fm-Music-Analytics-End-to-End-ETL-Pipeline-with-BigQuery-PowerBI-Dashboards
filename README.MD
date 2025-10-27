## 📋 Overview

Proyek ini merupakan **end-to-end data analytics pipeline** yang mencakup proses pengambilan, transformasi, dan visualisasi data musik dari Last.fm API. Pipeline ini mengambil data top artists, tracks, dan tags secara global maupun khusus Indonesia, kemudian menyajikannya dalam dashboard interaktif untuk mendapatkan insights tentang tren musik terkini.

### Key Features
- ✅ *Data Extraction** dari Last.fm API dengan Prefect orchestration
- ✅ **Data Transformation** dengan Pandas untuk cleaning dan standardisasi
- ✅ **Cloud Data Warehouse** menggunakan Google BigQuery
- ✅ **Dual Dashboard** di Looker Studio dan Power BI
- ✅ **Global & Local Insights** untuk perbandingan tren musik dunia vs Indonesia

## 🏗️ Architecture

```
Last.fm API → Python (ETL) → Google BigQuery → Looker Studio / Power BI
                  ↑
              Prefect
           (Orchestration)
```

### Data Flow
1. **Extract**: Mengambil data dari Last.fm API menggunakan 5 endpoints:
   - `geo.gettopartists` (Indonesia)
   - `geo.gettoptracks` (Indonesia)
   - `chart.gettopartists` (Global)
   - `chart.gettoptracks` (Global)
   - `chart.gettoptags` (Global)

2. **Transform**: Membersihkan dan standardisasi data dengan Pandas

3. **Load**: Upload ke BigQuery dengan auto-detect schema

4. **Visualize**: Dashboard real-time di Looker Studio & Power BI

## 📊 Dashboard Insights

Dashboard menyajikan 4 metrik utama:

### Global Metrics
- **Artist's Global Listeners**: 527.6M total listeners
- **Track's Global Listeners**: 162.3M total listeners
- **Top Global Artists**: Coldplay, Radiohead, Lady Gaga, dll
- **Top Global Tracks**: Creep, No Surprises, Feel Good Inc., dll
- **Top Genres**: Alternative, rock, alternative rock, indie, pop, electronic



## 📦 Installation

### Prerequisites
- Python 3.9 atau lebih tinggi
- Google Cloud Project dengan BigQuery API enabled
- Last.fm API Key
- Google Cloud Service Account dengan BigQuery permissions

### Setup Steps

1. **Clone repository**
```bash
git clone https://github.com/yourusername/lastfm-music-analytics.git
cd lastfm-music-analytics
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Setup environment variables**

Buat file `.env` di root directory:
```env
LASTFM_API_KEY=your_lastfm_api_key
GCP_PROJECT_ID=your_gcp_project_id
BQ_DATASET=your_bigquery_dataset
GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account-key.json
```

4. **Setup Google Cloud**
- Buat service account di GCP Console
- Download JSON key file
- Grant BigQuery Admin role
- Update path di `.env`

5. **Create BigQuery Dataset**
```bash
bq mk --dataset your_project_id:your_dataset_name


## 🔍 Key Insights from Dashboard

1. **Global Dominance**: Coldplay memimpin dengan 17.9M listeners, diikuti Radiohead dan Kanye West
2. **Indonesia Preference**: Pop international mendominasi dengan Taylor Swift dan Ariana Grande di puncak
3. **Genre Distribution**: Alternative dan rock masih menjadi genre paling populer secara global
4. **Track Popularity**: "Creep" oleh Radiohead menjadi track paling populer dengan 7.2M listeners
