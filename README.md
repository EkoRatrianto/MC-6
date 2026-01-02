# Hitung Tutupan Lahan (Python 3.11)

Repositori kecil ini adalah porting dari workflow n8n **HitungTutupanLahan-02** (file JSON) ke skrip Python 3.11.
Fungsinya setara dengan alur berikut:

1. Membuat daftar **20 periode kuartalan terakhir** (≈ 5 tahun) sampai **kuartal terakhir yang sudah selesai**.
2. Untuk setiap kuartal, melakukan **HTTP POST** ke *satellite API* untuk mengambil statistik tutupan lahan.
3. Menghitung **Tree / Non-Tree / Total** (ha) dan persentase **Non-Tree**.
4. Menghasilkan **laporan Markdown** berbahasa Indonesia (ringkasan tren + tabel kuartalan + rekomendasi).
5. (Opsional) Menyimpan hasil ke **Google Sheets** sebagai 1 baris: `generatedAt`, `reportMarkdown`, `resultsJson`.

Catatan penting:
- Seperti node n8n `HTTP Request` dengan `neverError=true`, skrip **tidak mematikan proses** hanya karena status non-2xx. Status code tetap dicatat.
- Bila `nonTreeAreaHa` tidak tersedia namun `totalAreaHa` tersedia, skrip akan menghitung `nonTreeAreaHa = totalAreaHa - treeAreaHa` (sesuai logika node `Calculate Non-Tree Coverage Area`).

## Prasyarat

- Python **3.11**
- Akses ke endpoint API statistik tutupan lahan (lihat `SATELLITE_API_BASE_URL`)
- (Opsional) Kredensial Google Sheets (service account) bila ingin menulis hasil ke Sheet

## Instalasi

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
source .venv/bin/activate

pip install -r requirements.txt
```

## Konfigurasi

Skrip membaca konfigurasi dari environment variables (bisa memakai file `.env`).

Minimal yang perlu Anda set:

- `SATELLITE_API_BASE_URL`  
  Contoh: `https://example.com/api/landcover-stats`

Opsional (memakai default dari workflow n8n):

- `AOI_JSON` : GeoJSON Polygon/Multipolygon dalam bentuk string JSON.
- `PIXEL_SIZE_METERS` : default `10`
- `TREE_NDVI_THRESHOLD` : default `0.5`
- `CLOUD_COVER_MAX` : default `20`
- `TIME_ZONE` : default `Asia/Jakarta`
- `REPORT_LOCALE` : default `id_ID`

Google Sheets (opsional):
- `GOOGLE_SHEET_ID`
- `GOOGLE_SHEET_TAB_NAME`
- `GOOGLE_SERVICE_ACCOUNT_FILE` : path file JSON service account (atau pakai `GOOGLE_APPLICATION_CREDENTIALS`)

Contoh `.env`:

```env
SATELLITE_API_BASE_URL=https://example.com/api/landcover-stats
AOI_JSON={"type":"Polygon","coordinates":[[[106.7,-6.3],[106.7,-6.1],[107.0,-6.1],[107.0,-6.3],[106.7,-6.3]]]}
PIXEL_SIZE_METERS=10
TREE_NDVI_THRESHOLD=0.5
CLOUD_COVER_MAX=20

# Opsional - Google Sheets
GOOGLE_SHEET_ID=REPLACE_WITH_YOUR_SHEET_ID
GOOGLE_SHEET_TAB_NAME=Quarterly_LandCover_Report
GOOGLE_SERVICE_ACCOUNT_FILE=/path/service-account.json
```

## Menjalankan

Menjalankan sekali (mengambil 20 kuartal terakhir dan menghasilkan output):

```bash
python hitung_tutupan_lahan.py --outdir ./out
```

Output yang dihasilkan:
- `out/results.json` : daftar hasil per kuartal
- `out/report.md` : laporan Markdown
- `out/payload.json` : payload yang siap ditulis ke Google Sheets

Jika konfigurasi Google Sheets lengkap, skrip juga akan melakukan append 1 baris ke tab yang ditentukan.

## Menjadwalkan eksekusi kuartalan (setara Schedule Trigger)

Di workflow n8n, trigger di-set setiap **3 bulan jam 02:00**. Di Linux Anda bisa meniru dengan cron:

```cron
0 2 1 */3 * /path/to/python /path/to/hitung_tutupan_lahan.py --outdir /path/to/out >> /path/to/out/run.log 2>&1
```

## Kontrak API yang diasumsikan

Request JSON (per kuartal):

```json
{
  "aoi": {...GeoJSON...},
  "startDate": "YYYY-MM-DD",
  "endDate": "YYYY-MM-DD",
  "cloudCoverMax": 20,
  "treeNdviThreshold": 0.5,
  "pixelSizeMeters": 10
}
```

Response minimal (mendukung beberapa variasi nama field, sesuai workflow):
- `treeAreaHa` atau `tree_area_ha` (atau variasi lain yang mirip)
- `totalAreaHa` atau `total_area_ha` (opsional bila `nonTreeAreaHa` tersedia)
- `nonTreeAreaHa` atau `non_tree_area_ha` (opsional)

## Catatan keamanan & operasional

- Hindari menaruh kredensial service account di repository publik.
- Tambahkan rate limit/backoff bila endpoint API memiliki pembatasan kuota.
