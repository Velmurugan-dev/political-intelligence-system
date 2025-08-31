# 🏛️ AIADMK Political Intelligence System

**Complete Automated Intelligence System for AIADMK Political Monitoring**

A production-ready, comprehensive political intelligence system designed specifically for monitoring AIADMK (All India Anna Dravida Munnetra Kazhagam) political activities across all major digital platforms.

## 🎯 Key Features

- 🔍 **Automated Content Discovery** - Uses SerpAPI and Brave Search for intelligent URL discovery
- 🤖 **Multi-Platform Scraping** - Powered by specialized Apify actors for reliable data extraction
- 📰 **Tamil News Processing** - Advanced content extraction from Tamil news sources with Firecrawl
- 🏛️ **AIADMK-Focused Intelligence** - Tailored for AIADMK political monitoring and analysis
- 📊 **Real-time Analytics** - Advanced engagement scoring and political sentiment tracking
- 🗄️ **Production Database** - Supabase PostgreSQL with party-platform table segregation
- ⚡ **Scalable Architecture** - Concurrent processing with intelligent rate limiting
- 🔄 **Continuous Monitoring** - Automated scheduling with health checks and error recovery
- 🎯 **Comprehensive Coverage** - YouTube, Facebook, Instagram, Twitter, Reddit, Tamil News
- 📱 **Professional CLI** - Production-ready command-line interface with full system management

## 📁 Project Structure

```
aiadmk_intelligence/
├── 🏛️ Core System
│   ├── aiadmk_intelligence_engine.py     # Main orchestration engine
│   ├── run_aiadmk_intelligence.py        # CLI interface
│   ├── config.py                         # Configuration management
│   ├── database.py                       # Supabase integration
│   └── schema.py                         # Database schema
├── 🔧 Services
│   ├── services/serpapi_service.py       # Google search & discovery
│   ├── services/brave_search_service.py  # Web content discovery
│   ├── services/firecrawl_service.py     # Tamil news extraction
│   └── services/apify_service.py         # Unified Apify integration
├── 📱 Platform Scrapers
│   ├── scrapers/facebook_scraper.py      # Facebook posts & comments
│   ├── scrapers/youtube_scraper.py       # YouTube videos & channels
│   ├── scrapers/instagram_scraper.py     # Instagram posts & stories
│   ├── scrapers/twitter_scraper.py       # Twitter tweets & threads
│   ├── scrapers/reddit_scraper.py        # Reddit posts & discussions
│   └── scrapers/tamil_news_processor.py  # Tamil news articles
├── 📁 Configuration
│   ├── .env                              # API keys & credentials
│   └── logs/                             # System logs
└── 📚 Documentation
    └── README.md                         # Complete system documentation
```

## 🛠️ Installation

### Prerequisites

- Python 3.8 or higher
- MongoDB Atlas account (or local MongoDB)
- Git

### Step 1: Clone and Setup

```bash
# Clone the repository (or create the folder)
mkdir political_scraper
cd political_scraper

# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On Linux/Mac:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Install Playwright browsers
playwright install
```

### Step 2: Configure Environment

1. **MongoDB is already configured** in the `.env` file:
   ```
   MONGODB_URI=mongodb+srv://electoraai:Velmurgan2003@cluster0.z1rsnyr.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0
   ```

2. **Optional: Add API Keys** for enhanced scraping (Method 3):
   - Edit `.env` and add your API keys
   - You can get free API keys from:
     - [SerpAPI](https://serpapi.com/) - Web search
     - [Brave Search](https://brave.com/search/api/) - Web search
     - [Anthropic](https://console.anthropic.com/) - AI extraction
     - [OpenAI](https://platform.openai.com/) - AI extraction

   **Note**: The scraper works without these API keys - it will use other methods.

### Step 3: Test Setup

```bash
# Test system setup
python main.py --test

# Validate environment
python orchestrator.py --validate
```

## 🚀 Usage

### Quick Start Commands

```bash
# Test system configuration
python3 run_aiadmk_intelligence.py --test-config

# Run single intelligence cycle
python3 run_aiadmk_intelligence.py --single-cycle

# Start continuous monitoring (30min cycles)
python3 run_aiadmk_intelligence.py --continuous

# Check system status and metrics
python3 run_aiadmk_intelligence.py --status
```

### Advanced Operations

```bash
# Continuous monitoring with custom interval
python3 run_aiadmk_intelligence.py --continuous --interval 60

# Verbose logging for debugging
python3 run_aiadmk_intelligence.py --single-cycle --verbose

# Test individual components
python3 config.py                    # Test configuration
python3 database.py                  # Test database connection
python3 schema.py                    # Create database tables
```

### Comprehensive Scraping (All Parties)

```bash
# Scrape all platforms for all parties
python main.py --comprehensive

# Using orchestrator for parallel execution
python orchestrator.py --comprehensive --start 2025-06-01 --end 2025-06-07
```

### Custom Scraping

```bash
# Specific parties and platforms
python main.py --party AIADMK --platform youtube
python main.py --parties AIADMK DMK --platforms youtube twitter

# Using orchestrator
python orchestrator.py --custom --parties AIADMK DMK BJP --platforms youtube twitter
```

### Advanced Usage

```bash
# Test mode with small limits
python main.py --test --dry-run --limit 5

# List all configured competitors
python main.py --list-competitors

# Add new competitor dynamically
python main.py --add-competitor "NEW_PARTY" \
  --tamil-keywords "தமிழ்,கீவேர்ட்" \
  --english-keywords "new,party,keywords"
```

## 🔧 Configuration

### Adding New Political Parties

Edit `competitors.yaml`:

```yaml
NEW_PARTY:
  priority: false
  limits_multiplier: 1.0
  tamil_keywords:
    - "புதிய கட்சி"
    - "தலைவர் பெயர்"
  english_keywords:
    - "New Party"
    - "Leader Name"
```

The system automatically:
- Creates MongoDB collections for the new party
- Uses the keywords for scraping
- Applies appropriate data limits

### Configuring Limits

Edit `config.yaml`:

```yaml
default_limits:
  posts: 100
  comments: 500

enhanced_limits:
  posts: 200      # For priority parties
  comments: 1000
```

### Timeframe Examples

```bash
# Single day
--timeframe 2025-06-01 2025-06-01

# One week
--timeframe 2025-06-01 2025-06-07

# One month
--timeframe 2025-01-01 2025-01-31

# Full year
--timeframe 2024-01-01 2024-12-31

# Custom range
--timeframe 2023-05-15 2025-08-20
```

## 📊 Data Storage

### MongoDB Collections

For each party, the system creates:
- `{party}_youtube_posts` & `{party}_youtube_comments`
- `{party}_twitter_posts` & `{party}_twitter_comments`
- `{party}_facebook_posts` & `{party}_facebook_comments`
- `{party}_instagram_posts` & `{party}_instagram_comments`
- `{party}_reddit_posts` & `{party}_reddit_comments`
- `{party}_tamil_news_posts` & `{party}_tamil_news_comments`

Example for AIADMK:
- `aiadmk_youtube_posts`
- `aiadmk_twitter_posts`
- etc.

### Document Schema

```javascript
{
  "content_id": "unique_id",
  "title": "Post title",
  "text": "Content text",
  "url": "https://...",
  "author": "Username",
  "party": "AIADMK",
  "keywords_matched": ["அதிமுக", "EPS"],
  "extraction_method": "playwright",
  "scraped_at": "2025-06-09T10:30:00",
  "platform": "youtube",
  // Platform-specific fields...
}
```

## 🔍 4 Scraping Methods

### Method 1: Playwright (Browser Automation)
- Opens real browser in headless mode
- Handles JavaScript-heavy sites
- Anti-detection measures

### Method 2: Specialized Libraries
- **yt-dlp**: YouTube videos and metadata
- **snscrape**: Twitter posts without login
- **instaloader**: Instagram posts and stories
- **facebook-scraper**: Facebook public posts
- **pushshift**: Reddit historical data

### Method 3: Web APIs + AI
- **SerpAPI/Brave Search**: Find social media content
- **Firecrawl/Crawl4AI**: Extract page content
- **Anthropic/OpenAI/Gemini**: AI-powered extraction

### Method 4: Basic HTTP
- Simple requests + BeautifulSoup
- RSS feeds
- Fallback method

## 🐛 Troubleshooting

### Common Issues

1. **Import Errors**: Each scraper is standalone - no cross-imports needed

2. **MongoDB Connection**:
   ```python
   # Test connection
   from pymongo import MongoClient
   uri = "mongodb+srv://electoraai:Velmurgan2003@cluster0.z1rsnyr.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
   client = MongoClient(uri)
   client.admin.command('ping')
   ```

3. **Missing Dependencies**:
   ```bash
   pip install -r requirements.txt --upgrade
   playwright install
   ```

4. **Rate Limiting**: The system automatically handles rate limits with delays and retries

### Debug Mode

```bash
# Enable debug logging
export DEBUG_MODE=true
export LOG_LEVEL=DEBUG

# Run with verbose output
python main.py --test --limit 5
```

## 📈 Performance Tips

1. **Parallel Execution**: Use orchestrator for faster scraping
   ```bash
   python orchestrator.py --comprehensive
   ```

2. **Adjust Workers**: Edit `config.yaml`:
   ```yaml
   scraping:
     parallel_scrapers: 5  # Increase for more parallel execution
   ```

3. **Platform Priority**: Scrape most important platforms first
   ```bash
   python main.py --platforms tamil_news youtube --all-parties
   ```

## 🤝 Contributing

To add new features:

1. Each scraper must remain standalone
2. Implement all 4 methods where possible
3. Follow the existing document schema
4. Update `competitors.yaml` for new parties
5. Test thoroughly before deployment

## 📄 License

This project is for educational and research purposes. Ensure you comply with each platform's Terms of Service and robots.txt.

## 🆘 Support

For issues:
1. Check the troubleshooting section
2. Verify all files are present: `python orchestrator.py --validate`
3. Check logs in `logs/scraper.log`
4. Ensure MongoDB connection is working

## 🎉 Quick Commands Reference

```bash
# AIADMK Priority (Recommended)
python main.py --aiadmk-priority --timeframe 2025-06-01 2025-06-07

# All Parties
python main.py --comprehensive

# Specific Platform
python youtube_scraper.py --party AIADMK

# Custom Timeframe (ANY dates)
python main.py --all-parties --timeframe 2024-01-01 2024-12-31

# Test Run
python main.py --test --limit 5

# Add New Party
python main.py --add-competitor "PARTY_NAME" --tamil-keywords "keyword1,keyword2" --english-keywords "keyword3,keyword4"
```

---

**Ready to start scraping!** 🚀