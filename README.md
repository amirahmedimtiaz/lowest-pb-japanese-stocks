# Lowest P/B Japanese Stocks Report

Automated tool to identify the top 20 stocks with the lowest positive P/B ratio in each sector across the Japanese market (JPX).

## Features
- Fetches all TSE-listed equities from JPX.
- Retrieves real-time P/B ratios and balance sheet data via Yahoo Finance.
- Calculates **NCA/BV Ratio** (Net Current Assets / Book Value) to evaluate value relative to tangible assets.
- Implements robust rate-limiting and 429 error handling for stable data fetching.
- Caches data locally to optimize subsequent runs.
- Automates daily analysis and email reporting via GitHub Actions.

## Setup
1. Clone the repository.
2. Install dependencies: `pip install -r requirements.txt`.
3. Create a `.env` file with the following credentials:
   ```env
   EMAIL_SENDER="your-email@gmail.com"
   EMAIL_PASSWORD="your-app-password"
   EMAIL_RECEIVER="recipient-email@gmail.com"
   ```
4. Run the analysis: `python lowest_pb_japanese_stocks.py`.

## Automation
The analysis runs daily at 08:00 AM JST (23:00 UTC) via GitHub Actions.
