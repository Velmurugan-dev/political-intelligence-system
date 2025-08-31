#!/bin/bash

# Political Scraper Installation Script
# This script installs all dependencies without problematic packages

echo "🚀 Political Scraper Installation"
echo "================================"

# Make scripts executable
chmod +x install.sh test_reddit.py check_dependencies.py main.py orchestrator.py

# Check Python version
python_version=$(python3 --version 2>&1 | awk '{print $2}')
echo "✓ Python version: $python_version"

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "🔄 Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo "📦 Upgrading pip..."
pip install --upgrade pip

# Install core dependencies first
echo "📦 Installing core dependencies..."
pip install python-dotenv PyYAML

# Install database
echo "🗄️ Installing MongoDB driver..."
pip install 'pymongo[srv]' motor

# Install Playwright
echo "🎭 Installing Playwright..."
pip install playwright>=1.49.0
playwright install

# Install specialized libraries (without Reddit API packages)
echo "📚 Installing specialized libraries..."
pip install yt-dlp snscrape instaloader facebook-scraper

# Install web scraping tools
echo "🌐 Installing web scraping tools..."
pip install aiohttp requests beautifulsoup4 lxml feedparser

# Install content extraction
echo "📰 Installing content extraction tools..."
pip install newspaper3k trafilatura

# Install utilities
echo "🔧 Installing utilities..."
pip install fake-useragent python-dateutil pytz tqdm tenacity

# Optional: Install AI services (only if you have API keys)
echo "🤖 Installing AI services (optional)..."
pip install google-search-results || echo "⚠️ google-search-results failed, continuing..."
pip install anthropic || echo "⚠️ anthropic failed, continuing..."
pip install openai || echo "⚠️ openai failed, continuing..."
pip install google-generativeai || echo "⚠️ google-generativeai failed, continuing..."

echo ""
echo "✅ Installation complete!"
echo ""

# Run dependency checker
echo "🔍 Checking installed dependencies..."
python check_dependencies.py

echo ""
echo "📋 Next steps:"
echo "1. Activate the virtual environment: source venv/bin/activate"
echo "2. Test the setup: python main.py --test"
echo "3. Start scraping: python main.py --aiadmk-priority"
echo ""
echo "💡 Note: The scraper works without AI API keys - they're optional."