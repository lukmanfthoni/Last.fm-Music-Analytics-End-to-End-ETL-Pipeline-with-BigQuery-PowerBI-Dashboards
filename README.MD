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



