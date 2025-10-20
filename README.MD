# EVDS DataHub

[![Günlük EVDS Veri Güncelleme](https://github.com/<Fetiiii>/evds-datahub/actions/workflows/data_update.yml/badge.svg)](../../actions/workflows/data_update.yml)

TCMB **EVDS** veritabanındaki binlerce ekonomik seriyi **otomatik çeken, düzenleyen ve güncel tutan** açık kaynaklı bir veri pipeline projesi.

- ⚙️ **Tam otomatik:** GitHub Actions ile periyodik güncelleme (`--update`)
- 💾 **Klasör yapısı:** Ana kategori → Alt kategori → Seri (CSV)
- 🔁 **Skip & Resume:** Var olan CSV’leri **atlayarak kaldığı yerden devam**
- 🧱 **Sağlamlık:** Bağlantı hatalarında **retry**, **loglama**, hata kaydı
- 🚀 **Kolay entegrasyon:** Ham verilerden ML/analiz projeleri için kaynak oluşturur

> 💡 Bu repo yalnızca **veri çekimi ve otomasyon** için tasarlandı.

---

## 📦 İçindekiler
- [Kurulum](#kurulum)
- [Kullanım](#kullanım)
- [Workflow (otomasyon)](#workflow-otomasyon)
- [Klasör Yapısı](#klasör-yapısı)
- [Loglama](#loglama)
- [Sorun Giderme](#sorun-giderme)
- [Lisans](#lisans)

---

## ⚙️ Kurulum

### 1️⃣ Bağımlılıklar
```bash
pip install -r requirements.txt

2️⃣ Ortam Değişkeni Ayarı
Lokal ortam:
.env dosyası oluştur:
EVDS_API_KEY=senin_api_anahtarın
ve main.py’nin başına (varsa) load_dotenv() ekle.

GitHub Actions ortamı:
Repo → Settings → Secrets and variables → Actions → New repository secret
Name: EVDS_API_KEY
Value: ( senin TCMB EVDS API anahtarın)

🚀 Kullanım
🔹 Tam çekim ( ilk kurulum veya tam veri yenileme)
python main.py
2000 yılından itibaren tüm verileri çeker.
CSV dosyası zaten varsa atlar (skip), yoksa oluşturur.

🔹 Güncelleme modu (workflow veya manuel)
python main.py --update
Varsayılan olarak son 3 günü kontrol eder.(UPDATE_DAYS keyfi olarak değiştirilebilir.)
Sadece yeni veri varsa ekler, tekrar eden tarihleri eler.
Günlük workflow’larda önerilen moddur.


🤖 Workflow (otomasyon)
.github/workflows/data_update.yml dosyası:
Manuel çalıştırma: GitHub Actions sekmesinden Run workflow
Zamanlama (isteğe bağlı): Her gece 03:00’te otomatik çalışacak şekilde ayarlandı.
Push sırasında GITHUB_TOKEN kimliğiyle otomatik commit yapılır.

⚠️ Not: Repo boyutu 2GB’ı aşarsa GitHub push zorlaşabilir.
Gerekirse yalnızca data/example/ klasörünü repoda tut veya Hugging Face Datasets entegrasyonunu kullan.



🗂️ Klasör Yapısı
data/
  Ana Kategori/
    Alt Kategori/
      Seri Adı.csv
logs/
  failed_series.txt      # Hata alan serilerin kayıtları
main.py                  # Çekim & güncelleme scripti
.github/workflows/
  data_update.yml        # Günlük/manuel update otomasyonu
requirements.txt         # Gerekli kütüphaneler


CSV dosya örneği:
Tarih, TP_DK_USD_A_YTL
2025-10-19, 32.45
2025-10-20, 32.52
...
Kolon adları otomatik olarak EVDS koduna göre oluşturulur (. yerine _).



🧾 Loglama
Tüm başarısız istekler logs/failed_series.txt dosyasına kaydedilir:
[2025-10-21 03:12:45] MENKUL KIYMET İSTATİSTİKLERİ | 1.1.6. Genel Yönetim Dışındaki Sektörlerce İhraç Edilen Borçlanma Senetleri | TP.MENKUL.BORC.STOK | ConnectionError veya boş veri
Bağlantı hataları: script otomatik olarak birkaç kez dener (retry)
API limiti (Too Many Requests): 60 saniye bekler ve yeniden dener
Başarısız denemeler: log dosyasına işlenir


🧩 Sorun Giderme
Hata    Açıklama / Çözüm
No such file or directory	Windows’ta uzun path hatası olabilir. LongPathsEnabled=1 aç.
Too Many Requests	API rate-limit. SLEEP_BETWEEN_SERIES değerini artır (ör. 1.0 → 2.0).
Push hatası (kimlik)	Branch korumasını veya GITHUB_TOKEN erişimini kontrol et.

📡 GÜNCELLEME MODU başlatılıyor...
🔍 Ana Kategori: PİYASA VERİLERİ (TCMB) (ID: 1)
  ▪ Alt Kategori: Açık Piyasa Repo ve Ters Repo İşlemleri
    • Seri: (1 GÜN) Ağırlıklı Ortalama Faiz (TP.API.REP.ORT.G1)
      ↪ Güncelleme aralığı: 18-10-2025 → 21-10-2025
      🔄 Güncellendi: data/Piyasa Verileri (TCMB)/Açık Piyasa Repo ve Ters Repo İşlemleri/(1 GÜN) Ağırlıklı Ortalama Faiz.csv (toplam 309 satır)


📘 Lisans
MIT Lisansı altında dağıtılmaktadır.
Bu projeyi kullanırken TCMB EVDS veri servisinin lisans ve kullanım şartlarını dikkate alınız.


🧠 Yazar Notu
EVDS DataHub, TCMB EVDS API’sinden alınan ekonomik serileri otomatik çekmek, organize etmek ve açık biçimde paylaşmak için oluşturulmuştur.
Veri mühendisliği, otomasyon ve açık veri altyapısı örneği olarak tasarlanmıştır.



