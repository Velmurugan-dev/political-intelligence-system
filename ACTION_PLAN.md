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

### Stage 2: Pipeline Tables ⏳
- [ ] **stage_results**: Raw discovery data with deduplication
- [ ] **final_results**: Enriched engagement data with metrics
- [ ] **manual_queue**: User-inserted URLs before processing
- [ ] **monitoring_schedule**: Scheduled monitoring jobs

### Stage 3: Advanced Tables ⏳  
- [ ] **scraping_jobs**: Queue management with Celery integration
- [ ] **deduplication_cache**: URL/content similarity tracking
- [ ] **analytics_summary**: Pre-computed metrics for dashboards

---

## 🔄 **2-Stage Pipeline Implementation**

### 🔍 **Stage 1: Discovery Engine** ⏳
**Goal**: Find URLs from keywords/sources and store in staging

#### Discovery Methods:
- [ ] **SerpAPI Integration**: Search competitor keywords across platforms
- [ ] **Brave Search Integration**: Alternative web search for URL discovery  
- [ ] **Firecrawl Integration**: News website crawling for articles
- [ ] **Manual URL Queue**: User-submitted URLs via frontend
- [ ] **Source Monitoring**: Regular checks of known channels/accounts

#### Implementation Tasks:
- [ ] `discovery_engine.py`: Main discovery orchestrator
- [ ] `keyword_discovery.py`: SerpAPI/Brave search automation
- [ ] `source_monitoring.py`: Channel monitoring automation
- [ ] `manual_queue_processor.py`: Process user-submitted URLs
- [ ] `discovery_deduplication.py`: URL-level deduplication logic

### ⚡ **Stage 2: Engagement Engine** ⏳
**Goal**: Scrape engagement metrics from discovered URLs

#### Engagement Extraction:
- [ ] **Apify Integration**: Reliable scraping via specialized actors
- [ ] **Browser Automation**: Fallback scraping for complex cases
- [ ] **API Integration**: Direct platform APIs where available
- [ ] **Content Analysis**: Extract comments, shares, views, likes
- [ ] **Growth Tracking**: Multiple snapshots over time

#### Implementation Tasks:
- [ ] `engagement_engine.py`: Main engagement orchestrator
- [ ] `apify_integration.py`: Enhanced Apify service with queue support
- [ ] `browser_automation.py`: Playwright/Selenium fallback scrapers
- [ ] `engagement_deduplication.py`: Content-level similarity detection
- [ ] `metrics_calculator.py`: Engagement rate and trend calculations

---

## 🚀 **Queue System Implementation**

### ⚙️ **Celery + Redis Setup** ⏳
- [ ] **Redis Configuration**: Queue backend setup
- [ ] **Celery Workers**: Background task processing
- [ ] **Task Routing**: Discovery vs Engagement task separation
- [ ] **Error Handling**: Retry logic and dead letter queues
- [ ] **Monitoring**: Task status and performance tracking

#### Implementation Tasks:
- [ ] `celery_config.py`: Celery application setup
- [ ] `celery_tasks.py`: Task definitions for discovery/engagement
- [ ] `queue_manager.py`: Queue monitoring and management
- [ ] `worker_health.py`: Worker status and restart logic

---

## 📅 **Manual & Monitoring Features**

### 🖱️ **Manual URL Management** ⏳
- [ ] **Frontend Interface**: Simple web UI for URL submission
- [ ] **Bulk Import**: CSV/Excel file upload for multiple URLs
- [ ] **URL Validation**: Check URL format and accessibility
- [ ] **Status Tracking**: Real-time processing status updates

### 📊 **Source Monitoring** ⏳
- [ ] **Channel Registration**: Add YouTube channels, FB pages, news sites
- [ ] **Scheduled Crawling**: Daily/weekly monitoring jobs
- [ ] **New Content Detection**: Compare against previous crawls
- [ ] **Alert System**: Notifications for high-engagement content

#### Implementation Tasks:
- [ ] `manual_interface.py`: Flask/FastAPI web interface
- [ ] `source_manager.py`: Channel/source registration system
- [ ] `monitoring_scheduler.py`: Cron-like job scheduling
- [ ] `alert_system.py`: Notification system for important content

---

## 🔄 **Multi-Competitor Architecture**

### 🏛️ **Competitor Management** ⏳
- [ ] **Dynamic Competitor Support**: Add/remove parties via configuration
- [ ] **Keyword Management**: Per-party keyword customization
- [ ] **Priority System**: Resource allocation based on importance
- [ ] **Cross-Competitor Analytics**: Comparative metrics and trends

#### Competitor Database:
- **ADMK**: அதிமுக, AIADMK, Edappadi Palaniswami, EPS
- **DMK**: திமுக, DMK, Stalin, MK Stalin  
- **BJP**: பாஜக, BJP, Tamil Nadu BJP
- **NTK**: நாம் தமிழர், NTK, Seeman
- **PMK**: பட்டாலி, PMK, Anbumani Ramadoss
- **TVK**: தளபதி, TVK, Vijay
- **DMDK**: டிஎம்டிகே, DMDK, Premalatha Vijayakanth

#### Implementation Tasks:
- [ ] `competitor_manager.py`: Dynamic competitor management
- [ ] `keyword_optimizer.py`: AI-powered keyword suggestion
- [ ] `priority_allocator.py`: Resource distribution logic
- [ ] `competitor_analytics.py`: Cross-party comparison metrics

---

## 🧹 **Deduplication System**

### 🔍 **Multi-Level Deduplication** ⏳

#### URL-Level (Hard Rules):
- [ ] **Canonical URL Normalization**: Remove tracking parameters
- [ ] **URL Similarity Detection**: Detect redirects and shortened URLs
- [ ] **Platform-Specific Logic**: Handle platform URL variations
- [ ] **Unique Constraint Enforcement**: Database-level duplicate prevention

#### Content-Level (Soft Rules): 
- [ ] **Text Similarity**: Trigram/cosine similarity for content matching
- [ ] **Image Hash Comparison**: Perceptual hashing for media content
- [ ] **Metadata Matching**: Title + author + date similarity
- [ ] **AI-Powered Detection**: LLM-based semantic similarity

#### Implementation Tasks:
- [ ] `url_normalizer.py`: URL cleaning and standardization
- [ ] `content_similarity.py`: Text and media similarity detection
- [ ] `dedup_engine.py`: Main deduplication orchestrator
- [ ] `similarity_cache.py`: Performance optimization with caching

---

## 📊 **Analytics & Reporting System**

### 📈 **Future Analytics Support** ⏳
- [ ] **Volume Metrics**: Mentions, posts, engagement over time
- [ ] **Content Analysis**: Keyword frequency, topic modeling
- [ ] **Sentiment Analysis**: Political sentiment tracking
- [ ] **Influence Metrics**: Channel ranking, reach analysis
- [ ] **Competitive Intelligence**: Share of voice, relative performance

#### Implementation Tasks:
- [ ] `analytics_engine.py`: Main analytics processor
- [ ] `sentiment_analyzer.py`: Tamil + English sentiment analysis
- [ ] `trend_detector.py`: Viral content and trend identification  
- [ ] `influence_calculator.py`: Channel authority and reach metrics
- [ ] `dashboard_data.py`: API endpoints for frontend dashboards

---

## 📱 **Frontend Dashboard** (Optional)

### 🖥️ **Web Interface** ⏳
- [ ] **Real-time Dashboard**: Live metrics and status updates
- [ ] **Manual URL Interface**: Submit URLs and monitor status
- [ ] **Analytics Views**: Charts, trends, competitor comparisons
- [ ] **Admin Panel**: System configuration and monitoring
- [ ] **Mobile Responsive**: Works on phones and tablets

#### Implementation Tasks:
- [ ] `dashboard_app.py`: Main Flask/FastAPI application
- [ ] `api_endpoints.py`: REST API for data access
- [ ] `frontend_templates/`: HTML/CSS/JS dashboard files
- [ ] `mobile_responsive.css`: Mobile optimization
- [ ] `real_time_updates.js`: WebSocket integration for live data

---

## 🔧 **Testing & Quality Assurance**

### ✅ **Testing Strategy** ⏳
- [ ] **Unit Tests**: Individual component testing
- [ ] **Integration Tests**: End-to-end pipeline testing
- [ ] **Load Tests**: Performance under high volume
- [ ] **Data Quality Tests**: Deduplication accuracy validation
- [ ] **API Tests**: External service integration testing

#### Implementation Tasks:
- [ ] `test_discovery.py`: Discovery engine test suite
- [ ] `test_engagement.py`: Engagement scraping tests
- [ ] `test_deduplication.py`: Deduplication accuracy tests
- [ ] `test_performance.py`: Load and performance testing
- [ ] `test_data_quality.py`: Data validation and integrity tests

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

## ✅ **Task Completion Tracking**

### 🎯 **Current Status**: 15% Complete
- ✅ **Codebase Analysis**: Completed
- ✅ **Action Plan**: Completed  
- ⏳ **Database Migration**: In Progress
- ❌ **Queue System**: Not Started
- ❌ **Discovery Engine**: Not Started
- ❌ **Engagement Engine**: Not Started
- ❌ **Deduplication**: Not Started
- ❌ **Analytics**: Not Started
- ❌ **Frontend**: Not Started
- ❌ **Testing**: Not Started

---

## 🌐 **Web UI Requirements** ⏳

### 📊 **Admin Dashboard Interface** 
- [ ] **Competitor Management**: Add/edit/delete political competitors
- [ ] **Keyword Management**: Manage keywords per competitor per platform
- [ ] **Manual URL Insertion**: Submit URLs for immediate processing
- [ ] **Channel Monitoring**: Add/monitor social media channels and news sources
- [ ] **Real-time Status**: Live view of scraping progress and results
- [ ] **Analytics Dashboard**: Charts, metrics, competitor comparisons

### 🔧 **Technical Requirements**
- [ ] **Frontend Framework**: React/Vue.js or Flask templates
- [ ] **Backend API**: FastAPI or Flask REST endpoints
- [ ] **Real-time Updates**: WebSocket integration for live data
- [ ] **Mobile Responsive**: Works on tablets and phones
- [ ] **Authentication**: Basic admin login system

---

## 📊 **Apify Integration Enhancement** ⏳

### 📁 **Results Analysis** 
- [ ] **Analyze Apify results in Results folder**: Understand data structure
- [ ] **Field Mapping**: Map Apify fields to database schema
- [ ] **Data Validation**: Ensure data quality and completeness
- [ ] **Error Handling**: Process failed scraping attempts

---

## 🗄️ **Database Cleanup & Migration** ⏳

### 🧹 **Supabase Cleanup**
- [ ] **Delete unwanted tables**: Remove AIADMK-specific tables
- [ ] **Backup existing data**: Export current data before migration
- [ ] **Create normalized schema**: Implement multi-competitor tables
- [ ] **Data migration**: Move existing data to new structure

---

## 📦 **Version Control Setup** ⏳

### 🔧 **GitHub Integration**
- [ ] **Initialize Git repository**: Setup version control
- [ ] **Create GitHub repository**: Connect to remote repository
- [ ] **Setup .gitignore**: Exclude sensitive files and logs
- [ ] **Initial commit**: Commit current codebase
- [ ] **Branch strategy**: Setup development branches

---

## 🚨 **Updated Implementation Priority**

### **Phase 0: Setup & Analysis (Week 1)** ⏳
- [ ] Analyze Apify results for field mapping
- [ ] Initialize GitHub repository for version control
- [ ] Clean up unwanted Supabase tables
- [ ] Update action plan with detailed tasks

### **Phase 1: Foundation (Weeks 1-2)** ⏳
- [ ] Database schema migration to multi-competitor
- [ ] Basic queue system setup (Celery + Redis)
- [ ] URL deduplication logic implementation
- [ ] Apify integration enhancement with proper field mapping

### **Phase 2: Core Pipeline (Weeks 2-3)** ⏳
- [ ] Discovery engine implementation
- [ ] Engagement engine enhancement
- [ ] Content deduplication system
- [ ] Basic web UI framework setup

### **Phase 3: Web Interface (Weeks 3-4)** ⏳
- [ ] Web UI for competitor management
- [ ] Web UI for keyword management  
- [ ] Web UI for manual URL insertion
- [ ] Web UI for channel monitoring
- [ ] Real-time dashboard implementation

### **Phase 4: Advanced Features (Weeks 4-5)** ⏳
- [ ] Source monitoring automation
- [ ] Analytics and reporting system
- [ ] Mobile-responsive design
- [ ] Performance optimization

### **Phase 5: Testing & Deployment (Week 5-6)** ⏳
- [ ] Comprehensive testing suite
- [ ] Load testing and optimization
- [ ] Production deployment setup
- [ ] Documentation and training

---

## ✅ **Detailed Task Completion Tracking**

### 🎯 **Current Status**: 25% Complete

#### ✅ **Completed Tasks**
- [x] **Codebase Analysis**: Full analysis of existing system
- [x] **Action Plan Creation**: Comprehensive roadmap created  
- [x] **Task Breakdown**: Detailed implementation tasks identified

#### ⏳ **In Progress**
- [ ] **Action Plan Updates**: Adding new requirements and priorities

#### 🔄 **Phase 0: Setup & Analysis** (0/4 completed)
- [ ] **Analyze Apify Results**: Understand data structure from Results folder
- [ ] **GitHub Repository Setup**: Initialize version control
- [ ] **Supabase Table Cleanup**: Remove unwanted tables
- [ ] **Updated Requirements Analysis**: Web UI and integration needs

#### 🏗️ **Phase 1: Foundation** (0/4 completed)
- [ ] **Database Schema Migration**: Multi-competitor normalized schema
- [ ] **Queue System Implementation**: Celery + Redis setup
- [ ] **Deduplication System**: URL and content duplicate prevention
- [ ] **Enhanced Apify Integration**: Field mapping and processing

#### 🔍 **Phase 2: Core Pipeline** (0/4 completed)
- [ ] **Discovery Engine**: Stage 1 URL discovery system
- [ ] **Engagement Engine**: Stage 2 metrics extraction system
- [ ] **Content Processing**: Advanced content analysis
- [ ] **Basic Web Framework**: Foundation for UI components

#### 🖥️ **Phase 3: Web Interface** (0/5 completed)
- [ ] **Competitor Management UI**: Add/edit/delete competitors
- [ ] **Keyword Management UI**: Manage keywords per competitor/platform
- [ ] **Manual URL Interface**: Submit URLs for processing
- [ ] **Channel Monitoring UI**: Monitor social media accounts
- [ ] **Real-time Dashboard**: Live status and analytics

#### 📊 **Phase 4: Advanced Features** (0/4 completed)
- [ ] **Source Monitoring Automation**: Scheduled monitoring jobs
- [ ] **Analytics Engine**: Metrics, trends, competitive intelligence
- [ ] **Mobile Optimization**: Responsive design for all devices
- [ ] **Performance Tuning**: Optimization for production load

#### 🧪 **Phase 5: Testing & Deployment** (0/4 completed)
- [ ] **Testing Suite**: Unit, integration, and load tests
- [ ] **Quality Assurance**: Data validation and error handling
- [ ] **Production Setup**: Deployment configuration
- [ ] **Documentation**: User guides and API documentation

---

*This action plan is actively updated as implementation progresses. Each checkbox represents a specific deliverable.*