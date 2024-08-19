# pp-scraper

Portfolio performance compatible scraper for hungarian instruments.

Scraping is done every weekday around 17:00 UTC time as a batch job for all data sources and the results are uploaded to [pp-data](https://github.com/havasd/pp-data) repository.

Primarily features:
- daily price data scraping in a json format per instrument for the below data sources
- optional historical price generation

## Implemented Spiders

|Data source name                                             | Spider name | Notes |
| ----------------------------------------------------------- | ----------- | ----- |
| [Alfa](https://www.alfanyugdij.hu/arfolyamrajzolo/)         | alfa_nyugdij | Can scrape historical data |
| [Allianz](https://www.allianz.hu/hu_HU/penztarak/arfolyamok-hozamok-tkm.html) | allianz_nyugdij| Can scrape historical data |
| [Aranykor](https://www.aranykornyp.hu/public/arfolyamok)    | aranykor    | Scrapes historical data |
| [Bamosz](https://www.bamosz.hu/legfrissebb-adatok)          | bamosz      | Supports historical scraping with splash |
| [Budapest](https://www.mbhbank.hu/onkentes-nyugdijpenztar/nyugdijpenztarak) | budapest_nyugdij | Can scrape historical data, scrapes VPF and PPF funds |
| [Erste](https://www.erstenyugdijpenztar.hu/fooldal)         | erste_nyugdij | Can scrape historical data from hand-crafted csv |
| [Honved](https://hnyp.hu/arfolyamok)                        | honved_nyugdij | Can scrape historical data |
| [Horizont](https://horizontmagannyugdijpenztar.hu/arfolyamok) | horizont_nyugdij | Can scrape historical data |
| [MÁK](https://www.allampapir.hu/kincstari_arfolyamjegyzes/) | mak         | Scrapes only latest data |
| [MÁK](https://www.allampapir.hu/kincstari_arfolyamjegyzes/) | mak_historical | Scrapes historical data from PDF report generator endpoint for a given time range. It uses tesseract OCR for extracting data from the PDF files. Best effort, the OCR makes some mistakes in certain cases for parsing tables |
| [MBH](https://www.mbhnyp.hu/arfolyamlekerdezes)             | mbh_nyugdij | Can scrape historical data |
| [OTP](https://www.otpnyugdij.hu/hu/arfolyamok)              | otp_nyugdij | Can scrape historical data |
| [Pannónia](https://www.pannonianyp.hu/arfolyamok/)          | pannonia_nyugdij | Can scrape historical data |
| [Szövetség](https://szovetsegnyp.hu/arfolyamok/megtekintes/)| szovetseg_nyugdij| Scrapes historical data from excel |

## Installation

For local execution you need to install the following packages.

### Ubuntu

1. Install `sudo apt-get install python3 python3-dev python3-pip libxml2-dev libxslt1-dev zlib1g-dev libffi-dev libssl-dev python3-venv docker.io tesseract-ocr`
2. `pip install -r requirements.txt`

