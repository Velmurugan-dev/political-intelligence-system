# 🎯 Political Scraping + ETL Pipeline Action Plan

## 📊 Current Codebase Analysis

### ✅ **Existing Structure (Current State)**
- **Database Layer**: Supabase PostgreSQL integration with AIADMK-focused schema
- **Scrapers**: Individual platform scrapers (Facebook, YouTube, Instagram, Twitter, Reddit, Tamil News)  
- **Services**: API integration services (SerpAPI, Brave Search, Firecrawl, Apify)
- **Configuration**: Environment-based config management system
- **Main Engine**: AIADMK Intelligence Engine with orchestration capabilities

### ❌ **Current Limitations** 
- **Single Party Focus**: Only supports AIADMK (needs multi-competitor support)
- **No Stage-Based Processing**: Direct scraping without discovery/engagement separation
- **No Manual URL Support**: No interface for manual URL insertion
- **Limited Monitoring**: No scheduled monitoring of known sources
- **No Deduplication**: Missing URL/content deduplication logic
- **No Queue System**: No proper job queue management (Celery + Redis)

---

## 🗂️ **Database Schema Transformation Plan**

### Stage 1: Core Tables Migration ✅
- [x] **competitors**: Multi-party support (ADMK, DMK, BJP, NTK, PMK, TVK, DMDK)
- [x] **platforms**: Normalized platform management 
- [x] **keywords**: Per competitor × platform keyword mapping
- [x] **sources**: Manual monitoring list (channels, accounts, websites)

### Stage 2: Pipeline Tables ✅
- [x] **stage_results**: Raw discovery data with deduplication
- [x] **final_results**: Enriched engagement data with metrics
- [x] **manual_queue**: User-inserted URLs before processing
- [x] **monitoring_schedule**: Scheduled monitoring jobs

### Stage 3: Advanced Tables ✅  
- [x] **scraping_jobs**: Queue management with Celery integration
- [x] **deduplication_cache**: URL/content similarity tracking (implemented in deduplication_engine)
- [x] **analytics_summary**: Pre-computed metrics for dashboards

---

## 🔄 **2-Stage Pipeline Implementation**

### 🔍 **Stage 1: Discovery Engine** ✅
**Goal**: Find URLs from keywords/sources and store in staging

#### Discovery Methods:
- [x] **SerpAPI Integration**: Search competitor keywords across platforms
- [x] **Brave Search Integration**: Alternative web search for URL discovery  
- [x] **Firecrawl Integration**: News website crawling for articles
- [x] **Manual URL Queue**: User-submitted URLs via frontend
- [x] **Source Monitoring**: Regular checks of known channels/accounts

#### Implementation Tasks:
- [x] `discovery_engine.py`: Main discovery orchestrator
- [x] `keyword_discovery.py`: SerpAPI/Brave search automation (integrated in discovery_engine)
- [x] `source_monitoring.py`: Channel monitoring automation (integrated in discovery_engine)
- [x] `manual_queue_processor.py`: Process user-submitted URLs (integrated in discovery_engine)
- [x] `discovery_deduplication.py`: URL-level deduplication logic (integrated in deduplication_engine)

### ⚡ **Stage 2: Engagement Engine** ✅
**Goal**: Scrape engagement metrics from discovered URLs

#### Engagement Extraction:
- [x] **Apify Integration**: Reliable scraping via specialized actors
- [x] **Browser Automation**: Fallback scraping for complex cases
- [x] **API Integration**: Direct platform APIs where available
- [x] **Content Analysis**: Extract comments, shares, views, likes
- [x] **Growth Tracking**: Multiple snapshots over time

#### Implementation Tasks:
- [x] `engagement_engine.py`: Main engagement orchestrator
- [x] `apify_integration.py`: Enhanced Apify service with queue support (integrated in engagement_engine)
- [x] `browser_automation.py`: Playwright/Selenium fallback scrapers (integrated in engagement_engine)
- [x] `engagement_deduplication.py`: Content-level similarity detection (integrated in deduplication_engine)
- [x] `metrics_calculator.py`: Engagement rate and trend calculations (integrated in engagement_engine)

---

## 🚀 **Queue System Implementation**

### ⚙️ **Celery + Redis Setup** ✅
- [x] **Redis Configuration**: Queue backend setup
- [x] **Celery Workers**: Background task processing
- [x] **Task Routing**: Discovery vs Engagement task separation
- [x] **Error Handling**: Retry logic and dead letter queues
- [x] **Monitoring**: Task status and performance tracking

#### Implementation Tasks:
- [x] `celery_config.py`: Celery application setup (celery_app.py)
- [x] `celery_tasks.py`: Task definitions for discovery/engagement (tasks.py)
- [x] `queue_manager.py`: Queue monitoring and management (integrated in main_orchestrator.py)
- [x] `worker_health.py`: Worker status and restart logic (integrated in main_orchestrator.py)

---

## 📅 **Manual & Monitoring Features**

### 🖱️ **Manual URL Management** ✅
- [x] **Frontend Interface**: Simple web UI for URL submission
- [x] **Bulk Import**: CSV/Excel file upload for multiple URLs
- [x] **URL Validation**: Check URL format and accessibility
- [x] **Status Tracking**: Real-time processing status updates

### 📊 **Source Monitoring** ✅
- [x] **Channel Registration**: Add YouTube channels, FB pages, news sites
- [x] **Scheduled Crawling**: Daily/weekly monitoring jobs
- [x] **New Content Detection**: Compare against previous crawls
- [x] **Alert System**: Notifications for high-engagement content

#### Implementation Tasks:
- [x] `manual_interface.py`: Flask/FastAPI web interface (web_ui/app.py)
- [x] `source_manager.py`: Channel/source registration system (integrated in web_ui and main_orchestrator)
- [x] `monitoring_scheduler.py`: Cron-like job scheduling (integrated in main_orchestrator)
- [x] `alert_system.py`: Notification system for important content (WebSocket integration in web_ui)

---

## 🔄 **Multi-Competitor Architecture**

### 🏛️ **Competitor Management** ✅
- [x] **Dynamic Competitor Support**: Add/remove parties via configuration
- [x] **Keyword Management**: Per-party keyword customization
- [x] **Priority System**: Resource allocation based on importance
- [x] **Cross-Competitor Analytics**: Comparative metrics and trends

#### Competitor Database:
- **ADMK**: அதிமுக, AIADMK, Edappadi Palaniswami, EPS
- **DMK**: திமுக, DMK, Stalin, MK Stalin  
- **BJP**: பாஜக, BJP, Tamil Nadu BJP
- **NTK**: நாம் தமிழர், NTK, Seeman
- **PMK**: பட்டாலி, PMK, Anbumani Ramadoss
- **TVK**: தளபதி, TVK, Vijay
- **DMDK**: டிஎம்டிகே, DMDK, Premalatha Vijayakanth

#### Implementation Tasks:
- [x] `competitor_manager.py`: Dynamic competitor management (web_ui/app.py)
- [x] `keyword_optimizer.py`: AI-powered keyword suggestion (integrated in web UI)
- [x] `priority_allocator.py`: Resource distribution logic (integrated in main_orchestrator)
- [x] `competitor_analytics.py`: Cross-party comparison metrics (analytics templates and API)

---

## 🧹 **Deduplication System**

### 🔍 **Multi-Level Deduplication** ✅

#### URL-Level (Hard Rules):
- [x] **Canonical URL Normalization**: Remove tracking parameters
- [x] **URL Similarity Detection**: Detect redirects and shortened URLs
- [x] **Platform-Specific Logic**: Handle platform URL variations
- [x] **Unique Constraint Enforcement**: Database-level duplicate prevention

#### Content-Level (Soft Rules): 
- [x] **Text Similarity**: Trigram/cosine similarity for content matching
- [x] **Image Hash Comparison**: Perceptual hashing for media content
- [x] **Metadata Matching**: Title + author + date similarity
- [x] **AI-Powered Detection**: LLM-based semantic similarity

#### Implementation Tasks:
- [x] `url_normalizer.py`: URL cleaning and standardization (deduplication_engine.py)
- [x] `content_similarity.py`: Text and media similarity detection (deduplication_engine.py)
- [x] `dedup_engine.py`: Main deduplication orchestrator (deduplication_engine.py)
- [x] `similarity_cache.py`: Performance optimization with caching (integrated in deduplication_engine.py)

---

## 📊 **Analytics & Reporting System**

### 📈 **Analytics Support** ✅
- [x] **Volume Metrics**: Mentions, posts, engagement over time
- [x] **Content Analysis**: Keyword frequency, topic modeling
- [x] **Sentiment Analysis**: Political sentiment tracking (basic implementation)
- [x] **Influence Metrics**: Channel ranking, reach analysis
- [x] **Competitive Intelligence**: Share of voice, relative performance

#### Implementation Tasks:
- [x] `analytics_engine.py`: Main analytics processor (integrated in main_orchestrator and web_ui)
- [x] `sentiment_analyzer.py`: Tamil + English sentiment analysis (basic implementation in analytics)
- [x] `trend_detector.py`: Viral content and trend identification (integrated in analytics)
- [x] `influence_calculator.py`: Channel authority and reach metrics (integrated in analytics)
- [x] `dashboard_data.py`: API endpoints for frontend dashboards (web_ui/app.py)

---

## 📱 **Frontend Dashboard** (Optional)

### 🖥️ **Web Interface** ✅
- [x] **Real-time Dashboard**: Live metrics and status updates
- [x] **Manual URL Interface**: Submit URLs and monitor status
- [x] **Analytics Views**: Charts, trends, competitor comparisons
- [x] **Admin Panel**: System configuration and monitoring
- [x] **Mobile Responsive**: Works on phones and tablets

#### Implementation Tasks:
- [x] `dashboard_app.py`: Main Flask/FastAPI application (web_ui/app.py)
- [x] `api_endpoints.py`: REST API for data access (web_ui/app.py)
- [x] `frontend_templates/`: HTML/CSS/JS dashboard files (web_ui/templates/)
- [x] `mobile_responsive.css`: Mobile optimization (integrated in templates)
- [x] `real_time_updates.js`: WebSocket integration for live data (integrated in templates)

---

## 🔧 **Testing & Quality Assurance**

### ✅ **Testing Strategy** ✅
- [x] **Unit Tests**: Individual component testing
- [x] **Integration Tests**: End-to-end pipeline testing
- [x] **Load Tests**: Performance under high volume
- [x] **Data Quality Tests**: Deduplication accuracy validation
- [x] **API Tests**: External service integration testing

#### Implementation Tasks:
- [x] `test_discovery.py`: Discovery engine test suite (test_discovery_engine.py)
- [x] `test_engagement.py`: Engagement scraping tests (test_engagement_engine.py)
- [x] `test_deduplication.py`: Deduplication accuracy tests (test_deduplication_engine.py)
- [x] `test_performance.py`: Load and performance testing (integrated in all test files)
- [x] `test_data_quality.py`: Data validation and integrity tests (integrated in test files)

---

## 📋 **Implementation Timeline**

### **Phase 1: Foundation (Weeks 1-2)** ⏳
- [ ] Database schema transformation
- [ ] Multi-competitor support
- [ ] Basic queue system setup
- [ ] URL deduplication logic

### **Phase 2: Core Pipeline (Weeks 3-4)** ⏳
- [ ] Discovery engine implementation
- [ ] Engagement engine enhancement
- [ ] Content deduplication system
- [ ] Manual URL interface

### **Phase 3: Monitoring & Analytics (Weeks 5-6)** ⏳
- [ ] Source monitoring system
- [ ] Basic analytics implementation
- [ ] Dashboard development
- [ ] Performance optimization

### **Phase 4: Advanced Features (Weeks 7-8)** ⏳
- [ ] AI-powered analytics
- [ ] Advanced deduplication
- [ ] Mobile dashboard
- [ ] Production deployment

---

## ✅ **Detailed Task Completion Tracking**

### 🎯 **Current Status**: 95% Complete

#### ✅ **Completed Tasks**
- [x] **Codebase Analysis**: Full analysis of existing system
- [x] **Action Plan Creation**: Comprehensive roadmap created  
- [x] **Task Breakdown**: Detailed implementation tasks identified
- [x] **Multi-Competitor Database Schema**: Normalized database design
- [x] **Discovery Engine**: Stage 1 URL discovery implementation
- [x] **Engagement Engine**: Stage 2 metrics extraction implementation
- [x] **Queue System**: Celery + Redis background processing
- [x] **Deduplication System**: Multi-level URL and content deduplication
- [x] **Web UI Framework**: Complete FastAPI-based web interface
- [x] **Analytics Dashboard**: Comprehensive analytics and reporting
- [x] **Source Monitoring**: Automated channel monitoring system
- [x] **Manual URL Processing**: User interface for manual submissions
- [x] **Real-time Updates**: WebSocket integration for live data
- [x] **Testing Suite**: Comprehensive unit, integration, and performance tests

#### ⏳ **Remaining Tasks**
- [ ] **Quality Assurance**: Data validation and error handling documentation
- [ ] **Production Setup**: Deployment configuration
- [ ] **Documentation**: User guides and API documentation

#### 🔄 **Phase 0: Setup & Analysis** (4/4 completed) ✅
- [x] **Analyze Apify Results**: Understand data structure from Results folder
- [x] **GitHub Repository Setup**: Initialize version control
- [x] **Supabase Table Cleanup**: Remove unwanted tables
- [x] **Updated Requirements Analysis**: Web UI and integration needs

#### 🏗️ **Phase 1: Foundation** (4/4 completed) ✅
- [x] **Database Schema Migration**: Multi-competitor normalized schema
- [x] **Queue System Implementation**: Celery + Redis setup
- [x] **Deduplication System**: URL and content duplicate prevention
- [x] **Enhanced Apify Integration**: Field mapping and processing

#### 🔍 **Phase 2: Core Pipeline** (4/4 completed) ✅
- [x] **Discovery Engine**: Stage 1 URL discovery system
- [x] **Engagement Engine**: Stage 2 metrics extraction system
- [x] **Content Processing**: Advanced content analysis
- [x] **Basic Web Framework**: Foundation for UI components

#### 🖥️ **Phase 3: Web Interface** (5/5 completed) ✅
- [x] **Competitor Management UI**: Add/edit/delete competitors
- [x] **Keyword Management UI**: Manage keywords per competitor/platform
- [x] **Manual URL Interface**: Submit URLs for processing
- [x] **Channel Monitoring UI**: Monitor social media accounts
- [x] **Real-time Dashboard**: Live status and analytics

#### 📊 **Phase 4: Advanced Features** (4/4 completed) ✅
- [x] **Source Monitoring Automation**: Scheduled monitoring jobs
- [x] **Analytics Engine**: Metrics, trends, competitive intelligence
- [x] **Mobile Optimization**: Responsive design for all devices
- [x] **Performance Tuning**: Optimization for production load

#### 🧪 **Phase 5: Testing & Deployment** (1/4 completed) ⏳
- [x] **Testing Suite**: Unit, integration, and load tests
- [ ] **Quality Assurance**: Data validation and error handling
- [ ] **Production Setup**: Deployment configuration
- [ ] **Documentation**: User guides and API documentation

---

*This action plan is actively updated as implementation progresses. Each checkbox represents a specific deliverable.*