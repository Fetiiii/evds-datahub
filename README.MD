# EVDS DataHub

[![Günlük EVDS Veri Güncelleme](https://github.com/Fetiiii/evds-datahub/actions/workflows/data_update.yml/badge.svg)](../../actions/workflows/data_update.yml)

TCMB **EVDS** veritabanındaki binlerce ekonomik seriyi **otomatik çeken, düzenleyen ve güncel tutan** açık kaynaklı bir veri pipeline projesi.

## ✨ Özellikler

- ⚙️ **Tam otomatik:** GitHub Actions ile periyodik güncelleme
- 💾 **Düzenli klasör yapısı:** Ana kategori → Alt kategori → Seri (CSV)
- 🔁 **Akıllı güncelleme:** Var olan CSV'leri atlayarak kaldığı yerden devam
- 🧱 **Sağlam altyapı:** Bağlantı hatalarında otomatik retry ve detaylı loglama
- 🚀 **Kolay entegrasyon:** ML/analiz projeleri için hazır veri kaynağı

> 💡 Bu repo yalnızca **veri çekimi ve otomasyon** için tasarlanmıştır.

---

## 📦 İçindekiler

- [Kurulum](#-kurulum)
- [Kullanım](#-kullanım)
  - [Tam Çekim](#-tam-çekim-ilk-kurulum)
  - [Güncelleme Modu](#-güncelleme-modu)
- [Workflow (Otomasyon)](#-workflow-otomasyon)
- [Klasör Yapısı](#️-klasör-yapısı)
- [Loglama](#-loglama)
- [Sorun Giderme](#-sorun-giderme)
- [Lisans](#-lisans)

---

## ⚙️ Kurulum

### 1️⃣ Bağımlılıkları Yükleyin

```bash
pip install -r requirements.txt
```

### 2️⃣ API Anahtarını Ayarlayın

#### Lokal Ortam

`.env` dosyası oluşturun:

```env
EVDS_API_KEY=senin_api_anahtarın
```

`main.py` dosyasının başına `load_dotenv()` ekleyin.

#### GitHub Actions Ortamı

1. Repo → **Settings** → **Secrets and variables** → **Actions**
2. **New repository secret** butonuna tıklayın
3. Bilgileri girin:
   - **Name:** `EVDS_API_KEY`
   - **Value:** TCMB EVDS API anahtarınız

---

## 🚀 Kullanım

### 🔹 Tam Çekim (İlk Kurulum)

İlk kurulum veya tam veri yenileme için:

```bash
python main.py
```

**Davranış:**
- 2000 yılından itibaren tüm verileri çeker
- CSV dosyası zaten varsa atlar (skip)
- Yeni seriler için CSV oluşturur

### 🔹 Güncelleme Modu

Günlük güncelleme veya workflow için:

```bash
python main.py --update
```

**Davranış:**
- Varsayılan olarak son 3 günü kontrol eder (`UPDATE_DAYS` değişkeni ile ayarlanabilir)
- Sadece yeni veri varsa ekler
- Tekrar eden tarihleri otomatik eler
- Günlük workflow'larda önerilen moddur

---

## 🤖 Workflow (Otomasyon)

`.github/workflows/data_update.yml` dosyası projeyi otomatikleştirir:

### Manuel Çalıştırma
GitHub → **Actions** sekmesi → **Run workflow** butonu

### Otomatik Zamanlama
Her gece **03:00**'te otomatik çalışacak şekilde ayarlanmıştır.

### Otomatik Commit
Push işlemi `GITHUB_TOKEN` kimliğiyle otomatik yapılır.

> ⚠️ **Uyarı:** Repo boyutu 2GB'ı aşarsa GitHub push zorlaşabilir. Gerekirse yalnızca `data/example/` klasörünü repoda tutun veya Hugging Face Datasets entegrasyonunu kullanın.

---

## 🗂️ Klasör Yapısı

```
evds-datahub/
├── data/
│   ├── Ana Kategori/
│   │   └── Alt Kategori/
│   │       └── Seri Adı.csv
│   └── ...
├── logs/
│   └── failed_series.txt
├── .github/
│   └── workflows/
│       └── data_update.yml
├── main.py
├── requirements.txt
└── README.md
```

### CSV Dosya Örneği

```csv
Tarih,TP_DK_USD_A_YTL
2025-10-19,32.45
2025-10-20,32.52
2025-10-21,32.58
```

Kolon adları otomatik olarak EVDS koduna göre oluşturulur (`.` yerine `_` kullanılır).

---

## 🧾 Loglama

Tüm başarısız istekler `logs/failed_series.txt` dosyasına kaydedilir:

```
[2025-10-21 03:12:45] MENKUL KIYMET İSTATİSTİKLERİ | 1.1.6. Genel Yönetim Dışındaki Sektörlerce İhraç Edilen Borçlanma Senetleri | TP.MENKUL.BORC.STOK | ConnectionError veya boş veri
```

### Hata Yönetimi

- **Bağlantı hataları:** Script otomatik olarak birkaç kez dener (retry)
- **API limiti (429):** 60 saniye bekler ve yeniden dener
- **Başarısız denemeler:** Log dosyasına kaydedilir

### Güncelleme Çıktı Örneği

```
📡 GÜNCELLEME MODU başlatılıyor...
🔍 Ana Kategori: PİYASA VERİLERİ (TCMB) (ID: 1)
  ▪ Alt Kategori: Açık Piyasa Repo ve Ters Repo İşlemleri
    • Seri: (1 GÜN) Ağırlıklı Ortalama Faiz (TP.API.REP.ORT.G1)
      ↪ Güncelleme aralığı: 18-10-2025 → 21-10-2025
      🔄 Güncellendi: data/Piyasa Verileri (TCMB)/.../(1 GÜN) Ağırlıklı Ortalama Faiz.csv (toplam 309 satır)
```

---

## 🛠️ Sorun Giderme

| Hata | Açıklama / Çözüm |
|------|------------------|
| `No such file or directory` | Windows'ta uzun path hatası olabilir. `LongPathsEnabled=1` ayarını aktifleştirin. |
| `Too Many Requests (429)` | API rate-limit. `SLEEP_BETWEEN_SERIES` değerini artırın (ör. `1.0` → `2.0`). |
| Push hatası (kimlik) | Branch korumasını veya `GITHUB_TOKEN` erişimini kontrol edin. |
| `API key hatası` | `.env` dosyanızın doğru konumda olduğundan ve `EVDS_API_KEY` değerinin geçerli olduğundan emin olun. |

---

## 📘 Lisans

Bu proje **MIT Lisansı** altında dağıtılmaktadır.

Bu projeyi kullanırken **TCMB EVDS** veri servisinin lisans ve kullanım şartlarını dikkate alınız.

---

## 🧠 Yazar Notu

**EVDS DataHub**, TCMB EVDS API'sinden alınan ekonomik serileri otomatik çekmek, organize etmek ve açık biçimde paylaşmak için oluşturulmuştur.

Veri mühendisliği, otomasyon ve açık veri altyapısı örneği olarak tasarlanmıştır.


<div align="center">

**⭐ Projeyi beğendiyseniz yıldız vermeyi unutmayın!**

</div>
