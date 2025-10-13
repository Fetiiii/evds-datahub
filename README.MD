# evds-datahub

TCMB EVDS'ten tüm serileri çekip `data/` klasöründe CSV olarak saklayan proje.

## Kullanım (lokalde)
1. EVDS API anahtarını al.
2. Ortam değişkeni olarak ayarla: `export EVDS_API_KEY="ANAHTARINIZ"`
3. Gerekli paketleri yükle: `pip install -r requirements.txt`
4. `python main.py` çalıştır.

## GitHub Actions
- `EVDS_API_KEY` adında secret tanımlayın.
- Workflow günlük otomatik olarak çalışacaktır.
