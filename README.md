# Value Seeker Backend

A Flask-based backend service for analyzing stock market data and related news articles.

## Overview

Value Seeker Backend is a comprehensive system that:
- Fetches and analyzes stock market data
- Collects relevant news articles from GDELT
- Processes news content using Gemini AI
- Identifies market inflection points
- Provides RESTful APIs for frontend consumption

## Prerequisites

- Python 3.8+
- Flask
- Google Cloud credentials for Gemini AI
- GDELT API access
- Environment variables configured

## Installation

1. Clone the repository
2. Create a virtual environment:
```python
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```python
pip install -r requirements.txt
```

4. Create a `.env` file with required credentials:
```
GEMINI_API_KEY=your_gemini_api_key
FLASK_DEBUG=True
```

## Project Structure

- `flask_stock/`: Main Flask application directory
  - `controllers/`: API endpoint controllers
  - `services/`: Business logic services
- `news_main.py`: Main script for news processing pipeline
- `k_line.py`: Stock price analysis and inflection point detection
- `json_indexer.py`: News article indexing and organization

## Usage

1. Start the Flask server:
```python
python main.py
```

2. Run the news processing pipeline:
```python
python news_main.py
```

The news processing pipeline (referenced in `news_main.py`) performs the following steps:
```python:value_seeker_backend/news_main.py
startLine: 76
endLine: 142
```

## API Endpoints

- `GET /api/kline`: Get K-line data for a specific stock
- `GET /api/news`: Get processed news data and inflection points
- `GET /health`: Health check endpoint

## License

MIT License - see LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## Error Handling

The application includes comprehensive error handling and logging. Logs are written to console and can be configured for file output.

## Configuration

Server configuration can be modified in `flask_stock/flask_settings.py`:
```python:value_seeker_backend/flask_stock/flask_settings.py
startLine: 1
endLine: 13
```