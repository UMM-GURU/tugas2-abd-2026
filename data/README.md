# Panduan Dataset — Tugas 2 (Data Product)

Folder ini digunakan untuk menempatkan dataset yang dipakai dalam dashboard analitik.

> **PENTING:** Jangan pernah melakukan `git commit` atau `git push` file dataset besar (>100MB) langsung ke repository GitHub! File data besar harus diabaikan melalui `.gitignore`.

---

## Informasi Dataset Kelompok

- **Nama Dataset:** Chicago Crime Dataset (2001–2024)
- **Sumber Data:** https://huggingface.co/datasets/gymprathap/Chicago-Crime-Dataset (City of Chicago Data Portal)
- **Ukuran File Mentah:** 1.825,6 MB (~1,83 GB)
- **Jumlah Baris:** 8.104.658 baris mentah | 8.014.867 baris bersih
- **Format Output:** Apache Parquet (kompresi Zstd), partisi Hive `data/processed/chicago_crimes/year=YYYY/data.parquet`

---

## Instruksi Pengunduhan & Penyiapan Data

```bash
mkdir -p data/raw data/processed

python -c "
from huggingface_hub import hf_hub_download
hf_hub_download(
    repo_id='gymprathap/Chicago-Crime-Dataset',
    filename='chicago_crimes.csv',
    repo_type='dataset',
    local_dir='data/raw'
)
"

python -m src.utils --input data/raw/chicago_crimes.csv --output data/processed/chicago_crimes
```

Untuk uji cepat satu partisi tahun saja (misal 2023):

```bash
python -m src.utils --input data/raw/chicago_crimes.csv --output data/processed/chicago_crimes --year 2023
```

## Skema Kolom Output Parquet

Kolom `year` dibaca otomatis oleh DuckDB melalui partisi folder `year=YYYY/`.

| Nama Kolom | Tipe Data | Deskripsi |
|------------|:---------:|-----------|
| `id` | Int64 | Identitas unik insiden (kunci deduplikasi) |
| `datetime` | Datetime | Tanggal dan waktu kejadian |
| `primary_type` | String | Kategori utama tindak kejahatan |
| `description` | String | Sub-kategori detail tindak kejahatan |
| `location_description` | String | Jenis lokasi kejadian |
| `arrest` | Boolean | Status penangkapan pelaku |
| `domestic` | Boolean | Indikator kasus kekerasan rumah tangga |
| `district` | Int16 | Nomor distrik kepolisian (nullable) |
| `beat` | Int16 | Kode area patroli kepolisian (nullable) |
| `ward` | Int16 | Wilayah administratif dewan kota (nullable) |
| `community_area` | Int16 | Kode 77 area komunitas Chicago (nullable) |
| `latitude` | Float32 | Lintang geografis (`41.60` s.d. `42.05`) |
| `longitude` | Float32 | Bujur geografis (`-87.95` s.d. `-87.50`) |