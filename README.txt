AnalisisDataPerusahaanTbk — Python 3.11

1) Install dependencies
   pip install -r requirements.txt

2) Credentials (environment variables)

   OpenAI:
   - OPENAI_API_KEY

   Google (recommended using service account):
   - GOOGLE_APPLICATION_CREDENTIALS=/path/to/service_account.json

   Optional bond API:
   - BOND_API_URL
   - BOND_API_TOKEN  (if required)

   Optional weekly schedule default payload:
   - DEFAULT_ANALYSIS_PAYLOAD_JSON='{"company_name":"...","stock_ticker":"...","google_sheet_id":"...","drive_folder_id":"..."}'

3) Run once
   - Create a payload.json that matches AnalysisRequest schema, example:

   {
     "company_name": "PT Bank Central Asia Tbk",
     "stock_ticker": "IDX:BBCA",
     "period_years": 5,
     "benchmark_ticker": "IHSG",
     "bond_identifiers": [],
     "annual_report_urls": ["https://.../annual-report-2023.pdf", "..."],
     "news_query": "BBCA",
     "google_sheet_id": "YOUR_SHEET_ID",
     "drive_folder_id": "YOUR_DRIVE_FOLDER_ID",
     "recipients_email": []
   }

   Then:
   python analisis_data_perusahaan_tbk_py311.py --input-json payload.json

   Outputs will be written to ./outputs/

4) Run as API server (webhook)
   python analisis_data_perusahaan_tbk_py311.py --serve

   POST http://localhost:8000/analisa-emiten
   Content-Type: application/json
   Body: same as payload.json

Notes / Limitations
- If annual_report_urls is empty, financial ratio extraction is skipped and report will contain limitations.
- Benchmark data is optional; if you keep benchmark in the same Google Sheet, create a sheet tab named "Benchmark Data".
- PDF text extraction quality varies by report formatting; consider using a dedicated OCR pipeline for scanned PDFs.
