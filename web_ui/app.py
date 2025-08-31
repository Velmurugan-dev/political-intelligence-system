#!/usr/bin/env python3
"""
Political Intelligence System - Web UI Application
FastAPI-based web interface for competitor management, keyword management,
manual URL insertion, and channel monitoring
"""

import os
import asyncio
import json
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, HttpUrl
import asyncpg

# Import our database and services
import sys
sys.path.append('..')
from database import get_database
from config import get_config

# Create FastAPI app
app = FastAPI(
    title="Political Intelligence Dashboard",
    description="Multi-competitor political intelligence monitoring system",
    version="1.0.0"
)

# Static files and templates
app.mount("/static", StaticFiles(directory="web_ui/static"), name="static")
templates = Jinja2Templates(directory="web_ui/templates")

# WebSocket connections for real-time updates
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    
    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
    
    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except:
                pass

manager = ConnectionManager()

# Pydantic models for API requests
class CompetitorCreate(BaseModel):
    name: str
    display_name: str
    short_code: str
    description: Optional[str] = None
    party_color: Optional[str] = "#4285f4"
    founded_year: Optional[int] = None
    headquarters: Optional[str] = None
    official_website: Optional[str] = None
    priority_level: int = 1

class KeywordCreate(BaseModel):
    competitor_id: int
    platform_id: int
    keyword: str
    language: str = "ta"
    keyword_type: str = "general"
    search_frequency_hours: int = 4

class SourceCreate(BaseModel):
    competitor_id: int
    platform_id: int
    name: str
    url: str
    source_type: str
    identifier: Optional[str] = None
    monitoring_frequency_hours: int = 6

class ManualURLSubmit(BaseModel):
    competitor_id: int
    platform_id: int
    url: str
    priority: int = 1
    notes: Optional[str] = None

# Database dependency
async def get_db():
    return await get_database()

# Routes
@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Main dashboard page"""
    return templates.TemplateResponse("dashboard.html", {"request": request})

@app.get("/competitors", response_class=HTMLResponse)
async def competitors_page(request: Request):
    """Competitors management page"""
    return templates.TemplateResponse("competitors.html", {"request": request})

@app.get("/keywords", response_class=HTMLResponse)
async def keywords_page(request: Request):
    """Keywords management page"""
    return templates.TemplateResponse("keywords.html", {"request": request})

@app.get("/manual-urls", response_class=HTMLResponse)
async def manual_urls_page(request: Request):
    """Manual URL submission page"""
    return templates.TemplateResponse("manual_urls.html", {"request": request})

@app.get("/monitoring", response_class=HTMLResponse)
async def monitoring_page(request: Request):
    """Channel monitoring page"""
    return templates.TemplateResponse("monitoring.html", {"request": request})

@app.get("/analytics", response_class=HTMLResponse)
async def analytics_page(request: Request):
    """Analytics and reporting page"""
    return templates.TemplateResponse("analytics.html", {"request": request})

# API Endpoints

# Competitors API
@app.get("/api/competitors")
async def get_competitors(db=Depends(get_db)):
    """Get all competitors"""
    query = "SELECT * FROM competitors ORDER BY priority_level, name"
    results = await db.execute_query(query)
    return [dict(row) for row in results]

@app.post("/api/competitors")
async def create_competitor(competitor: CompetitorCreate, db=Depends(get_db)):
    """Create new competitor"""
    query = """
    INSERT INTO competitors (name, display_name, short_code, description, party_color, founded_year, headquarters, official_website, priority_level)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    RETURNING competitor_id
    """
    try:
        result = await db.execute_query(query, (
            competitor.name, competitor.display_name, competitor.short_code,
            competitor.description, competitor.party_color, competitor.founded_year,
            competitor.headquarters, competitor.official_website, competitor.priority_level
        ))
        await manager.broadcast({"type": "competitor_added", "data": competitor.dict()})
        return {"success": True, "competitor_id": result[0]['competitor_id']}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/api/competitors/{competitor_id}")
async def delete_competitor(competitor_id: int, db=Depends(get_db)):
    """Delete competitor"""
    query = "DELETE FROM competitors WHERE competitor_id = $1"
    await db.execute_query(query, (competitor_id,))
    await manager.broadcast({"type": "competitor_deleted", "data": {"competitor_id": competitor_id}})
    return {"success": True}

# Platforms API
@app.get("/api/platforms")
async def get_platforms(db=Depends(get_db)):
    """Get all platforms"""
    query = "SELECT * FROM platforms ORDER BY category, name"
    results = await db.execute_query(query)
    return [dict(row) for row in results]

# Keywords API
@app.get("/api/keywords")
async def get_keywords(competitor_id: Optional[int] = None, platform_id: Optional[int] = None, db=Depends(get_db)):
    """Get keywords with optional filtering"""
    query = """
    SELECT k.*, c.name as competitor_name, p.name as platform_name 
    FROM keywords k
    JOIN competitors c ON k.competitor_id = c.competitor_id
    JOIN platforms p ON k.platform_id = p.platform_id
    WHERE 1=1
    """
    params = []
    
    if competitor_id:
        query += " AND k.competitor_id = $" + str(len(params) + 1)
        params.append(competitor_id)
    
    if platform_id:
        query += " AND k.platform_id = $" + str(len(params) + 1)
        params.append(platform_id)
    
    query += " ORDER BY c.name, p.name, k.keyword"
    
    results = await db.execute_query(query, tuple(params))
    return [dict(row) for row in results]

@app.post("/api/keywords")
async def create_keyword(keyword: KeywordCreate, db=Depends(get_db)):
    """Create new keyword"""
    query = """
    INSERT INTO keywords (competitor_id, platform_id, keyword, language, keyword_type, search_frequency_hours)
    VALUES ($1, $2, $3, $4, $5, $6)
    RETURNING keyword_id
    """
    try:
        result = await db.execute_query(query, (
            keyword.competitor_id, keyword.platform_id, keyword.keyword,
            keyword.language, keyword.keyword_type, keyword.search_frequency_hours
        ))
        await manager.broadcast({"type": "keyword_added", "data": keyword.dict()})
        return {"success": True, "keyword_id": result[0]['keyword_id']}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/api/keywords/{keyword_id}")
async def delete_keyword(keyword_id: int, db=Depends(get_db)):
    """Delete keyword"""
    query = "DELETE FROM keywords WHERE keyword_id = $1"
    await db.execute_query(query, (keyword_id,))
    await manager.broadcast({"type": "keyword_deleted", "data": {"keyword_id": keyword_id}})
    return {"success": True}

# Sources API
@app.get("/api/sources")
async def get_sources(competitor_id: Optional[int] = None, platform_id: Optional[int] = None, db=Depends(get_db)):
    """Get monitoring sources"""
    query = """
    SELECT s.*, c.name as competitor_name, p.name as platform_name 
    FROM sources s
    JOIN competitors c ON s.competitor_id = c.competitor_id
    JOIN platforms p ON s.platform_id = p.platform_id
    WHERE 1=1
    """
    params = []
    
    if competitor_id:
        query += " AND s.competitor_id = $" + str(len(params) + 1)
        params.append(competitor_id)
    
    if platform_id:
        query += " AND s.platform_id = $" + str(len(params) + 1)
        params.append(platform_id)
    
    query += " ORDER BY c.name, p.name, s.name"
    
    results = await db.execute_query(query, tuple(params))
    return [dict(row) for row in results]

@app.post("/api/sources")
async def create_source(source: SourceCreate, db=Depends(get_db)):
    """Create new monitoring source"""
    query = """
    INSERT INTO sources (competitor_id, platform_id, name, url, source_type, identifier, monitoring_frequency_hours)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    RETURNING source_id
    """
    try:
        result = await db.execute_query(query, (
            source.competitor_id, source.platform_id, source.name,
            source.url, source.source_type, source.identifier, source.monitoring_frequency_hours
        ))
        await manager.broadcast({"type": "source_added", "data": source.dict()})
        return {"success": True, "source_id": result[0]['source_id']}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/api/sources/{source_id}")
async def delete_source(source_id: int, db=Depends(get_db)):
    """Delete monitoring source"""
    query = "DELETE FROM sources WHERE source_id = $1"
    await db.execute_query(query, (source_id,))
    await manager.broadcast({"type": "source_deleted", "data": {"source_id": source_id}})
    return {"success": True}

# Manual URL API
@app.get("/api/manual-queue")
async def get_manual_queue(status: Optional[str] = None, db=Depends(get_db)):
    """Get manual URL queue"""
    query = """
    SELECT mq.*, c.name as competitor_name, p.name as platform_name 
    FROM manual_queue mq
    JOIN competitors c ON mq.competitor_id = c.competitor_id
    JOIN platforms p ON mq.platform_id = p.platform_id
    WHERE 1=1
    """
    params = []
    
    if status:
        query += " AND mq.status = $" + str(len(params) + 1)
        params.append(status)
    
    query += " ORDER BY mq.priority DESC, mq.created_at DESC"
    
    results = await db.execute_query(query, tuple(params))
    return [dict(row) for row in results]

@app.post("/api/manual-queue")
async def submit_manual_url(url_data: ManualURLSubmit, db=Depends(get_db)):
    """Submit manual URL for processing"""
    query = """
    INSERT INTO manual_queue (competitor_id, platform_id, url, priority, notes, submitted_by)
    VALUES ($1, $2, $3, $4, $5, $6)
    RETURNING queue_id
    """
    try:
        result = await db.execute_query(query, (
            url_data.competitor_id, url_data.platform_id, url_data.url,
            url_data.priority, url_data.notes, "web_ui"
        ))
        
        # Trigger processing
        await manager.broadcast({
            "type": "manual_url_submitted", 
            "data": {**url_data.dict(), "queue_id": result[0]['queue_id']}
        })
        
        return {"success": True, "queue_id": result[0]['queue_id']}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Analytics API
@app.get("/api/analytics/overview")
async def get_analytics_overview(db=Depends(get_db)):
    """Get analytics overview"""
    
    # Get recent data from stage_results and final_results
    stage_query = """
    SELECT 
        c.name as competitor,
        p.name as platform,
        COUNT(*) as discovered_urls,
        COUNT(CASE WHEN status = 'scraped' THEN 1 END) as processed_urls
    FROM stage_results sr
    JOIN competitors c ON sr.competitor_id = c.competitor_id
    JOIN platforms p ON sr.platform_id = p.platform_id
    WHERE sr.inserted_at >= NOW() - INTERVAL '7 days'
    GROUP BY c.name, p.name
    ORDER BY discovered_urls DESC
    """
    
    engagement_query = """
    SELECT 
        c.name as competitor,
        p.name as platform,
        COUNT(*) as total_content,
        SUM(views_count) as total_views,
        SUM(likes_count) as total_likes,
        SUM(comments_count) as total_comments,
        AVG(engagement_rate) as avg_engagement
    FROM final_results fr
    JOIN competitors c ON fr.competitor_id = c.competitor_id
    JOIN platforms p ON fr.platform_id = p.platform_id
    WHERE fr.scraped_at >= NOW() - INTERVAL '7 days'
    GROUP BY c.name, p.name
    ORDER BY total_engagement DESC
    """
    
    try:
        stage_results = await db.execute_query(stage_query)
        engagement_results = await db.execute_query(engagement_query)
        
        return {
            "discovery_stats": [dict(row) for row in stage_results],
            "engagement_stats": [dict(row) for row in engagement_results],
            "last_updated": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/analytics/trends")
async def get_trends(days: int = 7, db=Depends(get_db)):
    """Get trending content and metrics"""
    
    query = """
    SELECT 
        fr.title,
        fr.url,
        fr.author,
        c.name as competitor,
        p.name as platform,
        fr.views_count,
        fr.likes_count,
        fr.comments_count,
        fr.engagement_rate,
        fr.viral_score,
        fr.published_at
    FROM final_results fr
    JOIN competitors c ON fr.competitor_id = c.competitor_id
    JOIN platforms p ON fr.platform_id = p.platform_id
    WHERE fr.scraped_at >= NOW() - INTERVAL '%s days'
    ORDER BY fr.viral_score DESC, fr.engagement_rate DESC
    LIMIT 50
    """ % days
    
    try:
        results = await db.execute_query(query)
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Job Status API
@app.get("/api/jobs/status")
async def get_job_status(db=Depends(get_db)):
    """Get current job status"""
    query = """
    SELECT 
        job_type,
        status,
        COUNT(*) as count,
        AVG(progress) as avg_progress
    FROM scraping_jobs 
    WHERE created_at >= NOW() - INTERVAL '24 hours'
    GROUP BY job_type, status
    ORDER BY job_type, status
    """
    
    try:
        results = await db.execute_query(query)
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Real-time WebSocket
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # Keep connection alive and send periodic updates
            await asyncio.sleep(10)
            await websocket.send_json({
                "type": "heartbeat",
                "timestamp": datetime.now().isoformat()
            })
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# Background task for processing manual URLs
async def process_manual_url_background(queue_id: int):
    """Background task to process manual URL"""
    db = await get_database()
    
    try:
        # Update status to processing
        await db.execute_query(
            "UPDATE manual_queue SET status = 'processing', processed_at = NOW() WHERE queue_id = $1",
            (queue_id,)
        )
        
        # Get URL details
        result = await db.execute_query(
            "SELECT * FROM manual_queue WHERE queue_id = $1",
            (queue_id,)
        )
        
        if result:
            url_data = dict(result[0])
            
            # Insert into stage_results for processing
            stage_query = """
            INSERT INTO stage_results (competitor_id, platform_id, url, title, discovery_method, status)
            VALUES ($1, $2, $3, $4, 'manual', 'pending')
            RETURNING stage_id
            """
            
            stage_result = await db.execute_query(stage_query, (
                url_data['competitor_id'],
                url_data['platform_id'], 
                url_data['url'],
                f"Manual submission: {url_data['notes'] or 'No notes'}"
            ))
            
            # Update manual queue with stage_id
            await db.execute_query(
                "UPDATE manual_queue SET status = 'processed', stage_result_id = $1 WHERE queue_id = $2",
                (stage_result[0]['stage_id'], queue_id)
            )
            
            await manager.broadcast({
                "type": "manual_url_processed",
                "data": {"queue_id": queue_id, "stage_id": stage_result[0]['stage_id']}
            })
    
    except Exception as e:
        # Update status to failed
        await db.execute_query(
            "UPDATE manual_queue SET status = 'failed' WHERE queue_id = $1",
            (queue_id,)
        )
        
        await manager.broadcast({
            "type": "manual_url_failed",
            "data": {"queue_id": queue_id, "error": str(e)}
        })

# Health check
@app.get("/api/health")
async def health_check():
    """System health check"""
    try:
        db = await get_database()
        await db.execute_query("SELECT 1")
        return {"status": "healthy", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e), "timestamp": datetime.now().isoformat()}

if __name__ == "__main__":
    # Create directories if they don't exist
    Path("web_ui/templates").mkdir(parents=True, exist_ok=True)
    Path("web_ui/static/css").mkdir(parents=True, exist_ok=True)
    Path("web_ui/static/js").mkdir(parents=True, exist_ok=True)
    
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)