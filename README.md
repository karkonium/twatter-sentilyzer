# TwitterScrapper

### Overview
TwitterScrapper is a Python tool designed to scrape Twitter for cryptocurrency-related tweets, analyze sentiment, and upload the results to Google Drive.

### Features
- Twitter scraping using snscrape
- Sentiment analysis with VADER and text2emotion
- File upload to Google Drive folder

### Requirements 
- get necessary packages with `pip`
- fill up the necessary enviroment varaibles in `./config/.env`

### How to Run
```
python3 main.py <start-date> <end-date>
```
Where dates are in YYYY-MM-DD format, start-date must be earlier than end-date; also note that start-date is inclusive, end-date is exclusive.
