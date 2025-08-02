# Data Integration ETL - SP500 Financial Data (StockDat)

This project demonstrates an end-to-end **ETL (Extract, Transform, Load)** pipeline that pulls financial data for S&P 500 companies, performs basic financial metric extraction (e.g., Market Cap, P/E ratios, dividend yields), and stores the results in a **MongoDB** database for further analysis.

---

## Purpose

This project showcases the ETL pipline portion of the backend of a stock analytics platform by:

- Extracting key financial indicators for publicly traded companies (S&P 500).
- Enriching the data with financial statements (Balance Sheet, Income Statement).
- Performing calculations such as **ROE** and **ROA**.
- Persisting the structured data into MongoDB.

---

## Technologies Used

- Python
- Pandas
- Yahoo Finance API (`yfinance`)
- MongoDB + `pymongo`
- dotenv (for environment variables)
- Scheduling with `cron` (macOS)
- Selenium and BeautifulSoup for HTMl Web Scraping

---

## Key Metrics Extracted

- Market Cap (in billions)
- Trailing P/E, Forward P/E
- Trailing Annual Dividend Yield
- 5-Year Average Dividend Yield
- Payout Ratio
- Return on Equity (ROE)
- Return on Assets (ROA)
- TTM (Trailing Twelve Months) variants of ROE and ROA
- Annual Income Statements and Balance Sheets
- Quarterly Income Statements and Balance Sheets

---

## Example Output

Each MongoDB document for a company might look like:

{
"Symbol": "AAPL",
"MarketCap_Billions": 2875.32,
"TrailingPE": 29.12,
"ForwardPE": 27.10,
"TrailingAnnualDividendYield": 0.56,
"FiveYearAvgDividendYield": 0.89,
"PayoutRatio": 14.2,
"ROE": 138.9,
"ROA": 25.4,
"AnnualIncomeStatements": [],
"AnnualBalanceSheets": [],
"QuarterlyIncomeStatements": [],
"QuarterlyBalanceSheets" []
}

---

## Schedule Automation (macOS)

Schedule the ETL to run every Monday at 6 AM using cron:

crontab -e
