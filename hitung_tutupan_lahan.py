#!/usr/bin/env python3
"""
hitung_tutupan_lahan.py (Python 3.11)

Porting dari workflow n8n "HitungTutupanLahan-02".
- Menghasilkan 20 kuartal terakhir (5 tahun) sampai kuartal terakhir yang sudah selesai
- Memanggil API statistik tutupan lahan per kuartal
- Menghitung Tree/Non-Tree/Total area (ha) dan Non-Tree (%)
- Membuat laporan Markdown (Bahasa Indonesia)
- (Opsional) Append hasil ke Google Sheets
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

import requests
from babel.numbers import format_decimal

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None  # type: ignore


DEFAULT_AOI = {
    "type": "Polygon",
    "coordinates": [
        [
            [106.7, -6.3],
            [106.7, -6.1],
            [107.0, -6.1],
            [107.0, -6.3],
            [106.7, -6.3],
        ]
    ],
}


@dataclass(frozen=True)
class Config:
    aoi: Dict[str, Any]
    pixel_size_meters: int
    tree_ndvi_threshold: float
    cloud_cover_max: int
    satellite_api_base_url: str
    google_sheet_id: Optional[str]
    google_sheet_tab_name: Optional[str]
    time_zone: str
    report_locale: str

    @staticmethod
    def from_env() -> "Config":
        def _get_int(name: str, default: int) -> int:
            v = os.getenv(name)
            return default if v is None or v == "" else int(v)

        def _get_float(name: str, default: float) -> float:
            v = os.getenv(name)
            return default if v is None or v == "" else float(v)

        aoi_json = os.getenv("AOI_JSON")
        if aoi_json:
            try:
                aoi = json.loads(aoi_json)
            except json.JSONDecodeError as e:
                raise SystemExit(f"AOI_JSON tidak valid (JSON decode error): {e}") from e
        else:
            aoi = DEFAULT_AOI

        return Config(
            aoi=aoi,
            pixel_size_meters=_get_int("PIXEL_SIZE_METERS", 10),
            tree_ndvi_threshold=_get_float("TREE_NDVI_THRESHOLD", 0.5),
            cloud_cover_max=_get_int("CLOUD_COVER_MAX", 20),
            satellite_api_base_url=os.getenv("SATELLITE_API_BASE_URL", "https://example.com/api/landcover-stats"),
            google_sheet_id=os.getenv("GOOGLE_SHEET_ID"),
            google_sheet_tab_name=os.getenv("GOOGLE_SHEET_TAB_NAME", "Quarterly_LandCover_Report"),
            time_zone=os.getenv("TIME_ZONE", "Asia/Jakarta"),
            report_locale=os.getenv("REPORT_LOCALE", "id_ID"),
        )


def last_completed_quarter(now_utc: datetime) -> Tuple[int, int]:
    """
    Mengembalikan (year, quarter) untuk kuartal terakhir yang sudah selesai
    berdasarkan waktu UTC (mirip logika workflow n8n).
    """
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)

    current_q = ((now_utc.month - 1) // 3) + 1  # 1..4
    end_q = current_q - 1
    end_year = now_utc.year
    if end_q == 0:
        end_q = 4
        end_year -= 1
    return end_year, end_q


def quarter_start_utc(year: int, quarter: int) -> datetime:
    """
    Awal kuartal dalam UTC.
    Q1 => Jan 1, Q2 => Apr 1, Q3 => Jul 1, Q4 => Okt 1.
    """
    month = (quarter - 1) * 3 + 1
    return datetime(year, month, 1, 0, 0, 0, tzinfo=timezone.utc)


def generate_last_20_quarters(cfg: Config, now_utc: Optional[datetime] = None) -> List[Dict[str, Any]]:
    """
    Setara node "Generate Date List for 5 Years" (20 kuartal).
    Menghasilkan list dict: cfg + periodLabel + startDate + endDate.
    endDate dibuat inclusive (hari terakhir kuartal).
    """
    now_utc = now_utc or datetime.now(timezone.utc)
    end_year, end_q = last_completed_quarter(now_utc)

    periods: List[Dict[str, Any]] = []
    for i in range(19, -1, -1):
        q = end_q - i
        y = end_year
        while q <= 0:
            q += 4
            y -= 1

        start = quarter_start_utc(y, q)
        next_q = 1 if q == 4 else q + 1
        next_y = y + 1 if q == 4 else y
        end_exclusive = quarter_start_utc(next_y, next_q)
        end_inclusive = end_exclusive - timedelta(days=1)

        periods.append(
            {
                "aoi": cfg.aoi,
                "pixelSizeMeters": cfg.pixel_size_meters,
                "treeNdviThreshold": cfg.tree_ndvi_threshold,
                "cloudCoverMax": cfg.cloud_cover_max,
                "satelliteApiBaseUrl": cfg.satellite_api_base_url,
                "googleSheetId": cfg.google_sheet_id,
                "googleSheetTabName": cfg.google_sheet_tab_name,
                "timeZone": cfg.time_zone,
                "reportLocale": cfg.report_locale,
                "periodLabel": f"{y}-Q{q}",
                "startDate": start.date().isoformat(),
                "endDate": end_inclusive.date().isoformat(),
            }
        )
    return periods


def _safe_number(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def call_satellite_api(period: Dict[str, Any], timeout_s: int = 60) -> Dict[str, Any]:
    """
    Setara node "Fetch Satellite Imagery Data" + "Merge Request Context + Response".
    Mengembalikan dict yang menyertakan:
      - periodLabel/startDate/endDate (context)
      - statusCode
      - body (hasil json, jika bisa diparse)
      - rawText (fallback jika bukan JSON)
    """
    url = period["satelliteApiBaseUrl"]
    payload = {
        "aoi": period["aoi"],
        "startDate": period["startDate"],
        "endDate": period["endDate"],
        "cloudCoverMax": period["cloudCoverMax"],
        "treeNdviThreshold": period["treeNdviThreshold"],
        "pixelSizeMeters": period["pixelSizeMeters"],
    }

    headers = {"accept": "application/json", "content-type": "application/json"}

    status_code: Optional[int] = None
    body: Any = None
    raw_text: Optional[str] = None
    error: Optional[str] = None

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=timeout_s)
        status_code = resp.status_code
        try:
            body = resp.json()
        except Exception:
            raw_text = resp.text
            body = None
    except Exception as e:
        error = str(e)

    out = dict(period)
    out.update(
        {
            "statusCode": status_code,
            "body": body,
            "rawText": raw_text,
            "error": error,
        }
    )
    return out


def calculate_non_tree(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Setara node "Calculate Non-Tree Coverage Area".
    Mendukung beberapa variasi nama field pada response body.
    """
    body = row.get("body") if isinstance(row.get("body"), dict) else {}
    # Variasi field tree
    tree = (
        body.get("treeAreaHa")
        or body.get("tree_area_ha")
        or body.get("tree_ha")
        or body.get("treeHa")
        or 0
    )
    tree_f = float(tree) if _safe_number(tree) is not None else 0.0

    total = body.get("totalAreaHa") or body.get("total_area_ha") or body.get("total_ha") or body.get("totalHa")
    total_f = _safe_number(total)

    non_tree = body.get("nonTreeAreaHa") or body.get("non_tree_area_ha") or body.get("non_tree_ha") or body.get("nonTreeHa")
    non_tree_f = _safe_number(non_tree)

    if non_tree_f is None and total_f is not None:
        non_tree_f = total_f - tree_f
    if total_f is None and non_tree_f is not None:
        total_f = non_tree_f + tree_f

    non_tree_pct = (non_tree_f / total_f * 100.0) if (total_f and total_f > 0 and non_tree_f is not None) else None

    result = {
        "periodLabel": row.get("periodLabel"),
        "startDate": row.get("startDate"),
        "endDate": row.get("endDate"),
        "treeAreaHa": tree_f,
        "nonTreeAreaHa": non_tree_f,
        "totalAreaHa": total_f,
        "nonTreePct": non_tree_pct,
        "apiStatusCode": row.get("statusCode"),
        "apiError": row.get("error"),
    }
    return result


def _fmt_num_id(value: Optional[float], locale_str: str, digits: int = 2) -> str:
    if value is None:
        return "data tidak tersedia"
    # Babel: locale "id_ID"
    return format_decimal(value, format=f"#,##0.{('0'*digits)}", locale=locale_str)


def _parse_period_label(period_label: str) -> Tuple[int, int]:
    # "YYYY-Qn"
    y, q = period_label.split("-Q")
    return int(y), int(q)


def generate_report_markdown(results: List[Dict[str, Any]], locale_str: str = "id_ID") -> str:
    """
    Menghasilkan laporan Markdown mirip output agent, tetapi deterministik (tanpa LLM).
    """
    # Urutkan hasil berdasarkan periodLabel
    results_sorted = sorted([r for r in results if r.get("periodLabel")], key=lambda r: _parse_period_label(str(r["periodLabel"])))

    available_pct = [r["nonTreePct"] for r in results_sorted if isinstance(r.get("nonTreePct"), (int, float))]

    bullets: List[str] = []
    if results_sorted:
        bullets.append(f"Jumlah periode: {len(results_sorted)} kuartal (≈ 5 tahun).")
    missing = sum(1 for r in results_sorted if r.get("nonTreeAreaHa") is None or r.get("totalAreaHa") is None)
    if missing:
        bullets.append(f"Terdapat {missing} periode dengan data parsial atau tidak tersedia.")
    if available_pct:
        first = next((r for r in results_sorted if isinstance(r.get("nonTreePct"), (int, float))), None)
        last = next((r for r in reversed(results_sorted) if isinstance(r.get("nonTreePct"), (int, float))), None)
        if first and last:
            delta = float(last["nonTreePct"]) - float(first["nonTreePct"])
            bullets.append(f"Perubahan Non-Tree (%) dari awal ke akhir periode: {_fmt_num_id(delta, locale_str)} poin persentase.")
        avg = sum(float(x) for x in available_pct) / len(available_pct)
        bullets.append(f"Rata-rata Non-Tree (%): {_fmt_num_id(avg, locale_str)}.")
        mx = max(float(x) for x in available_pct)
        mn = min(float(x) for x in available_pct)
        bullets.append(f"Rentang Non-Tree (%): {_fmt_num_id(mn, locale_str)} – {_fmt_num_id(mx, locale_str)}.")
        # Deteksi perubahan terbesar quarter-to-quarter (berdasarkan pct)
        diffs: List[Tuple[str, float]] = []
        prev = None
        for r in results_sorted:
            pct = r.get("nonTreePct")
            if prev is not None and isinstance(pct, (int, float)) and isinstance(prev.get("nonTreePct"), (int, float)):
                d = float(pct) - float(prev["nonTreePct"])
                diffs.append((str(r["periodLabel"]), d))
            prev = r
        if diffs:
            label_max, dmax = max(diffs, key=lambda t: abs(t[1]))
            bullets.append(f"Perubahan kuartalan terbesar pada {label_max}: {_fmt_num_id(dmax, locale_str)} poin persentase.")
    else:
        bullets.append("Tidak ada nilai Non-Tree (%) yang dapat dihitung dari respons API.")

    # Batasi 5-10 bullet
    bullets = bullets[:10]

    # Tabel
    lines = []
    lines.append("| Period | Start | End | Tree (ha) | Non-Tree (ha) | Non-Tree (%) |")
    lines.append("|---|---|---:|---:|---:|---:|")
    for r in results_sorted:
        lines.append(
            "| {period} | {start} | {end} | {tree} | {non_tree} | {pct} |".format(
                period=r.get("periodLabel", ""),
                start=r.get("startDate", ""),
                end=r.get("endDate", ""),
                tree=_fmt_num_id(_safe_number(r.get("treeAreaHa")), locale_str),
                non_tree=_fmt_num_id(_safe_number(r.get("nonTreeAreaHa")), locale_str),
                pct=_fmt_num_id(_safe_number(r.get("nonTreePct")), locale_str),
            )
        )

    # Rekomendasi (umum, tanpa mengarang data luar)
    recs = [
        "Validasi konsistensi definisi kelas 'Tree' vs 'Non-Tree' pada API (threshold NDVI, masking awan, dan resolusi piksel).",
        "Investigasi kuartal dengan perubahan terbesar untuk memastikan tidak ada artefak (mis. cloud contamination) atau perubahan metode pemrosesan.",
        "Jika memungkinkan, simpan juga metadata kualitas (mis. persentase awan aktual, jumlah citra) untuk audit dan interpretasi tren.",
        "Gunakan AOI yang sama secara konsisten dan pastikan koordinat mengikuti urutan dan sistem referensi yang benar.",
    ]

    md = []
    md.append("## A) Ringkasan tren")
    md.extend([f"- {b}" for b in bullets])
    md.append("")
    md.append("## B) Tabel kuartalan")
    md.extend(lines)
    md.append("")
    md.append("## C) Kesimpulan dan rekomendasi singkat")
    md.extend([f"- {r}" for r in recs[:5]])
    md.append("")
    return "\n".join(md)


def append_to_google_sheets(
    sheet_id: str,
    tab_name: str,
    generated_at: str,
    report_markdown: str,
    results_json: str,
    service_account_file: Optional[str] = None,
) -> None:
    """
    Append 1 baris ke Google Sheets.
    Membutuhkan service account JSON.
    """
    # Lazy import agar tetap bisa jalan tanpa dependensi/konfigurasi Sheets.
    try:
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
    except Exception as e:  # pragma: no cover
        raise RuntimeError("Dependency Google Sheets belum terpasang. Pastikan install requirements.txt") from e

    key_file = service_account_file or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not key_file:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_FILE atau GOOGLE_APPLICATION_CREDENTIALS belum diset.")

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(key_file, scopes=scopes)
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)

    values = [[generated_at, report_markdown, results_json]]
    body = {"values": values}

    # Range minimal: tab!A:C
    target_range = f"{tab_name}!A:C"

    service.spreadsheets().values().append(
        spreadsheetId=sheet_id,
        range=target_range,
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body=body,
    ).execute()


def run(cfg: Config, outdir: Path, timeout_s: int = 60, write_sheet: bool = True) -> Dict[str, Any]:
    periods = generate_last_20_quarters(cfg)
    results: List[Dict[str, Any]] = []

    for p in periods:
        merged = call_satellite_api(p, timeout_s=timeout_s)
        calc = calculate_non_tree(merged)
        results.append(calc)

    report_md = generate_report_markdown(results, locale_str=cfg.report_locale)

    payload = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "reportMarkdown": report_md,
        "resultsJson": json.dumps(results, ensure_ascii=False),
    }

    outdir.mkdir(parents=True, exist_ok=True)
    (outdir / "results.json").write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    (outdir / "report.md").write_text(report_md, encoding="utf-8")
    (outdir / "payload.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    # Append ke Sheets jika dikonfigurasi
    if write_sheet and cfg.google_sheet_id and cfg.google_sheet_id != "REPLACE_WITH_YOUR_SHEET_ID":
        try:
            append_to_google_sheets(
                sheet_id=cfg.google_sheet_id,
                tab_name=cfg.google_sheet_tab_name or "Quarterly_LandCover_Report",
                generated_at=payload["generatedAt"],
                report_markdown=payload["reportMarkdown"],
                results_json=payload["resultsJson"],
                service_account_file=os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE"),
            )
            payload["googleSheetsAppended"] = True
        except Exception as e:
            payload["googleSheetsAppended"] = False
            payload["googleSheetsError"] = str(e)
    else:
        payload["googleSheetsAppended"] = False

    return payload


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Hitung tutupan lahan kuartalan (porting dari n8n).")
    p.add_argument("--outdir", default="./out", help="Folder output (default: ./out)")
    p.add_argument("--timeout", type=int, default=60, help="Timeout HTTP request (detik)")
    p.add_argument("--no-sheets", action="store_true", help="Jangan append ke Google Sheets walaupun konfigurasi ada")
    p.add_argument("--no-dotenv", action="store_true", help="Jangan load .env")
    return p


def main() -> int:
    args = build_arg_parser().parse_args()

    if not args.no_dotenv and load_dotenv is not None:
        load_dotenv(override=False)

    cfg = Config.from_env()

    # Validasi minimum
    if not cfg.satellite_api_base_url or cfg.satellite_api_base_url.startswith("https://example.com"):
        print("Peringatan: SATELLITE_API_BASE_URL belum diset (masih default).", file=sys.stderr)

    payload = run(cfg, outdir=Path(args.outdir), timeout_s=args.timeout, write_sheet=not args.no_sheets)

    # Ringkas
    print(json.dumps({k: payload[k] for k in ["generatedAt", "googleSheetsAppended"] if k in payload}, ensure_ascii=False))
    if payload.get("googleSheetsError"):
        print(f"GoogleSheetsError: {payload['googleSheetsError']}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
