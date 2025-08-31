#!/usr/bin/env python3
"""
Database Migration Script
Migrates from old AIADMK-specific schema to new multi-competitor normalized schema
"""

import asyncio
import os
import sys
import logging
from datetime import datetime
from typing import Dict, List, Any
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.connection import DatabaseConnection

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseMigration:
    """
    Handles migration from old schema to new normalized multi-competitor schema
    """
    
    def __init__(self):
        self.db = DatabaseConnection()
        
        # Default competitors to create
        self.default_competitors = [
            {'name': 'ADMK', 'full_name': 'All India Anna Dravida Munnetra Kazhagam', 'priority_level': 1, 'is_active': True},
            {'name': 'DMK', 'full_name': 'Dravida Munnetra Kazhagam', 'priority_level': 1, 'is_active': True},
            {'name': 'BJP', 'full_name': 'Bharatiya Janata Party', 'priority_level': 2, 'is_active': True},
            {'name': 'NTK', 'full_name': 'Naam Tamilar Katchi', 'priority_level': 3, 'is_active': True},
            {'name': 'PMK', 'full_name': 'Pattali Makkal Katchi', 'priority_level': 3, 'is_active': True},
            {'name': 'TVK', 'full_name': 'Tamilaga Vettri Kazhagam', 'priority_level': 3, 'is_active': True},
            {'name': 'DMDK', 'full_name': 'Desiya Murpokku Dravida Kazhagam', 'priority_level': 4, 'is_active': True},
        ]
        
        # Default platforms to create
        self.default_platforms = [
            {'name': 'Facebook', 'api_identifier': 'facebook', 'is_active': True, 'supports_engagement': True},
            {'name': 'Twitter', 'api_identifier': 'twitter', 'is_active': True, 'supports_engagement': True},
            {'name': 'Instagram', 'api_identifier': 'instagram', 'is_active': True, 'supports_engagement': True},
            {'name': 'YouTube', 'api_identifier': 'youtube', 'is_active': True, 'supports_engagement': True},
            {'name': 'Telegram', 'api_identifier': 'telegram', 'is_active': True, 'supports_engagement': False},
            {'name': 'WhatsApp', 'api_identifier': 'whatsapp', 'is_active': False, 'supports_engagement': False},
            {'name': 'News Websites', 'api_identifier': 'news', 'is_active': True, 'supports_engagement': False},
        ]
    
    async def connect(self):
        """Connect to database"""
        await self.db.connect()
        logger.info("Connected to database for migration")
    
    async def disconnect(self):
        """Disconnect from database"""
        await self.db.close()
        logger.info("Disconnected from database")
    
    async def create_new_schema(self):
        """
        Create the new normalized schema tables
        """
        logger.info("Creating new normalized database schema...")
        
        # Read and execute the schema from new_schema.py
        schema_file = os.path.join(os.path.dirname(__file__), 'new_schema.py')
        
        if not os.path.exists(schema_file):
            raise FileNotFoundError("new_schema.py file not found")
        
        # Import and execute the schema
        import importlib.util
        spec = importlib.util.spec_from_file_location("new_schema", schema_file)
        new_schema = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(new_schema)
        
        # Execute the create_schema function
        await new_schema.create_schema(self.db)
        
        logger.info("New schema created successfully")
    
    async def populate_default_data(self):
        """
        Populate default competitors and platforms
        """
        logger.info("Populating default competitors and platforms...")
        
        # Insert competitors
        for competitor in self.default_competitors:
            try:
                await self.db.execute_query("""
                INSERT INTO competitors (name, full_name, priority_level, is_active, metadata)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (name) DO NOTHING
                """, [
                    competitor['name'],
                    competitor['full_name'],
                    competitor['priority_level'],
                    competitor['is_active'],
                    {'created_by': 'migration_script', 'created_at': datetime.utcnow().isoformat()}
                ])
                logger.info(f"Created competitor: {competitor['name']}")
            except Exception as e:
                logger.error(f"Failed to create competitor {competitor['name']}: {e}")
        
        # Insert platforms
        for platform in self.default_platforms:
            try:
                await self.db.execute_query("""
                INSERT INTO platforms (name, api_identifier, is_active, supports_engagement, metadata)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (api_identifier) DO NOTHING
                """, [
                    platform['name'],
                    platform['api_identifier'],
                    platform['is_active'],
                    platform['supports_engagement'],
                    {'created_by': 'migration_script', 'created_at': datetime.utcnow().isoformat()}
                ])
                logger.info(f"Created platform: {platform['name']}")
            except Exception as e:
                logger.error(f"Failed to create platform {platform['name']}: {e}")
        
        logger.info("Default data populated successfully")
    
    async def migrate_existing_data(self) -> Dict[str, int]:
        """
        Migrate any existing data from old tables to new schema
        This is a placeholder for actual data migration if old tables exist
        """
        logger.info("Checking for existing data to migrate...")
        
        migration_results = {
            'keywords_migrated': 0,
            'sources_migrated': 0,
            'results_migrated': 0,
            'manual_urls_migrated': 0
        }
        
        # Get competitor and platform IDs for migration
        competitors = await self.db.execute_query("SELECT id, name FROM competitors")
        platforms = await self.db.execute_query("SELECT id, name FROM platforms")
        
        competitor_map = {comp['name']: comp['id'] for comp in competitors}
        platform_map = {plat['name']: plat['id'] for plat in platforms}
        
        # Try to migrate from common old table patterns
        old_table_patterns = [
            'aiadmk_keywords',
            'keywords',
            'discovery_keywords',
            'aiadmk_sources',
            'sources',
            'monitoring_sources'
        ]
        
        for table_name in old_table_patterns:
            try:
                # Check if table exists
                check_query = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = $1
                );
                """
                exists_result = await self.db.execute_query(check_query, [table_name])
                
                if not exists_result[0]['exists']:
                    continue
                
                logger.info(f"Found old table: {table_name}, attempting migration...")
                
                # Attempt to migrate based on table type
                if 'keyword' in table_name.lower():
                    migrated = await self._migrate_keywords_table(table_name, competitor_map)
                    migration_results['keywords_migrated'] += migrated
                
                elif 'source' in table_name.lower():
                    migrated = await self._migrate_sources_table(table_name, competitor_map, platform_map)
                    migration_results['sources_migrated'] += migrated
                
            except Exception as e:
                logger.warning(f"Could not migrate table {table_name}: {e}")
        
        logger.info(f"Data migration completed: {migration_results}")
        return migration_results
    
    async def _migrate_keywords_table(self, table_name: str, competitor_map: Dict[str, int]) -> int:
        """
        Migrate keywords from old table structure
        """
        try:
            # Get all data from old table
            old_data = await self.db.execute_query(f"SELECT * FROM {table_name}")
            migrated_count = 0
            
            for row in old_data:
                try:
                    # Try to determine competitor (default to ADMK if unclear)
                    competitor_id = competitor_map.get('ADMK', 1)  # Default to ADMK
                    
                    # Extract keyword - look for common column names
                    keyword = None
                    for col in ['keyword', 'keywords', 'search_term', 'term', 'name']:
                        if col in row and row[col]:
                            keyword = row[col]
                            break
                    
                    if not keyword:
                        continue
                    
                    # Insert into new keywords table
                    await self.db.execute_query("""
                    INSERT INTO keywords (competitor_id, keyword, priority_level, is_active, metadata)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (competitor_id, keyword) DO NOTHING
                    """, [
                        competitor_id,
                        keyword,
                        row.get('priority_level', 1),
                        row.get('is_active', True),
                        {'migrated_from': table_name, 'original_id': row.get('id')}
                    ])
                    
                    migrated_count += 1
                    
                except Exception as e:
                    logger.warning(f"Failed to migrate keyword row: {e}")
            
            logger.info(f"Migrated {migrated_count} keywords from {table_name}")
            return migrated_count
            
        except Exception as e:
            logger.error(f"Failed to migrate keywords table {table_name}: {e}")
            return 0
    
    async def _migrate_sources_table(self, table_name: str, competitor_map: Dict[str, int], platform_map: Dict[str, int]) -> int:
        """
        Migrate sources from old table structure
        """
        try:
            old_data = await self.db.execute_query(f"SELECT * FROM {table_name}")
            migrated_count = 0
            
            for row in old_data:
                try:
                    # Default to ADMK competitor
                    competitor_id = competitor_map.get('ADMK', 1)
                    
                    # Try to determine platform from URL or type
                    platform_id = platform_map.get('Facebook', 1)  # Default to Facebook
                    url = row.get('url', '')
                    
                    if 'twitter.com' in url:
                        platform_id = platform_map.get('Twitter', platform_id)
                    elif 'instagram.com' in url:
                        platform_id = platform_map.get('Instagram', platform_id)
                    elif 'youtube.com' in url:
                        platform_id = platform_map.get('YouTube', platform_id)
                    
                    # Extract source name and URL
                    name = row.get('name', row.get('source_name', 'Migrated Source'))
                    
                    if not url:
                        continue
                    
                    # Insert into new sources table
                    await self.db.execute_query("""
                    INSERT INTO sources (competitor_id, platform_id, name, url, source_type, is_active, metadata)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (competitor_id, platform_id, url) DO NOTHING
                    """, [
                        competitor_id,
                        platform_id,
                        name,
                        url,
                        row.get('source_type', 'social_media'),
                        row.get('is_active', True),
                        {'migrated_from': table_name, 'original_id': row.get('id')}
                    ])
                    
                    migrated_count += 1
                    
                except Exception as e:
                    logger.warning(f"Failed to migrate source row: {e}")
            
            logger.info(f"Migrated {migrated_count} sources from {table_name}")
            return migrated_count
            
        except Exception as e:
            logger.error(f"Failed to migrate sources table {table_name}: {e}")
            return 0
    
    async def create_sample_data(self):
        """
        Create sample keywords and sources for testing
        """
        logger.info("Creating sample data for testing...")
        
        # Get competitor and platform IDs
        competitors = await self.db.execute_query("SELECT id, name FROM competitors WHERE is_active = true")
        platforms = await self.db.execute_query("SELECT id, name FROM platforms WHERE is_active = true")
        
        # Sample keywords for each competitor
        sample_keywords = {
            'ADMK': ['ஜெயலலிதா', 'அமமுக', 'எடப்பாடி பழனிசாமி', 'அ.தி.மு.க', 'AIADMK', 'Jayalalitha'],
            'DMK': ['ஸ்டாலின்', 'திமுக', 'கருணாநிதி', 'DMK', 'Stalin', 'Karunanidhi'],
            'BJP': ['மோடி', 'பாஜக', 'BJP', 'Modi', 'Narendra Modi', 'அண்ணாமலை'],
            'NTK': ['சீமான்', 'நாம் தமிழர்', 'NTK', 'Seeman', 'Naam Tamilar'],
        }
        
        # Sample sources for each platform
        sample_sources = {
            'Facebook': ['https://facebook.com/AIADMKOfficial', 'https://facebook.com/mkstalin'],
            'Twitter': ['https://twitter.com/AIADMKOfficial', 'https://twitter.com/mkstalin'],
            'YouTube': ['https://youtube.com/@AIADMKOfficial', 'https://youtube.com/@mkstalin'],
        }
        
        # Create sample keywords
        for comp in competitors[:4]:  # Only first 4 competitors
            comp_keywords = sample_keywords.get(comp['name'], [f"{comp['name']} keyword"])
            
            for keyword in comp_keywords:
                try:
                    await self.db.execute_query("""
                    INSERT INTO keywords (competitor_id, keyword, priority_level, is_active)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (competitor_id, keyword) DO NOTHING
                    """, [comp['id'], keyword, 1, True])
                except Exception as e:
                    logger.warning(f"Failed to create sample keyword {keyword}: {e}")
        
        # Create sample sources
        for plat in platforms[:3]:  # First 3 platforms
            plat_sources = sample_sources.get(plat['name'], [])
            
            for i, source_url in enumerate(plat_sources):
                try:
                    # Alternate between first two competitors
                    comp_id = competitors[i % 2]['id']
                    comp_name = competitors[i % 2]['name']
                    
                    await self.db.execute_query("""
                    INSERT INTO sources (competitor_id, platform_id, name, url, source_type, is_active)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (competitor_id, platform_id, url) DO NOTHING
                    """, [
                        comp_id,
                        plat['id'],
                        f"{comp_name} {plat['name']} Official",
                        source_url,
                        'social_media',
                        True
                    ])
                except Exception as e:
                    logger.warning(f"Failed to create sample source {source_url}: {e}")
        
        logger.info("Sample data created successfully")
    
    async def verify_migration(self) -> Dict[str, Any]:
        """
        Verify that the migration was successful
        """
        logger.info("Verifying migration...")
        
        verification = {
            'tables_created': [],
            'data_counts': {},
            'indexes_created': [],
            'constraints_created': [],
            'success': True,
            'errors': []
        }
        
        # Check all expected tables exist
        expected_tables = [
            'competitors', 'platforms', 'keywords', 'sources',
            'stage_results', 'final_results', 'manual_queue',
            'monitoring_schedule', 'scraping_jobs', 'analytics_summary',
            'system_logs'
        ]
        
        for table in expected_tables:
            try:
                check_query = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = $1
                );
                """
                exists_result = await self.db.execute_query(check_query, [table])
                
                if exists_result[0]['exists']:
                    verification['tables_created'].append(table)
                    
                    # Get row count
                    count_result = await self.db.execute_query(f"SELECT COUNT(*) as count FROM {table}")
                    verification['data_counts'][table] = count_result[0]['count']
                else:
                    verification['success'] = False
                    verification['errors'].append(f"Table {table} not found")
                    
            except Exception as e:
                verification['success'] = False
                verification['errors'].append(f"Error checking table {table}: {e}")
        
        # Check essential data exists
        if verification['data_counts'].get('competitors', 0) == 0:
            verification['errors'].append("No competitors found in database")
            verification['success'] = False
        
        if verification['data_counts'].get('platforms', 0) == 0:
            verification['errors'].append("No platforms found in database")
            verification['success'] = False
        
        logger.info(f"Migration verification: {'SUCCESS' if verification['success'] else 'FAILED'}")
        
        if verification['errors']:
            for error in verification['errors']:
                logger.error(f"Verification error: {error}")
        
        return verification

async def main():
    """Main migration function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Database Migration Tool')
    parser.add_argument('--skip-cleanup', action='store_true', help='Skip the cleanup step')
    parser.add_argument('--skip-migration', action='store_true', help='Skip old data migration')
    parser.add_argument('--create-samples', action='store_true', help='Create sample data for testing')
    parser.add_argument('--verify-only', action='store_true', help='Only verify existing schema')
    
    args = parser.parse_args()
    
    migration = DatabaseMigration()
    
    try:
        await migration.connect()
        
        if args.verify_only:
            verification = await migration.verify_migration()
            print("\n" + "="*50)
            print("MIGRATION VERIFICATION")
            print("="*50)
            print(f"Status: {'SUCCESS' if verification['success'] else 'FAILED'}")
            print(f"Tables created: {len(verification['tables_created'])}")
            
            if verification['data_counts']:
                print("\nData counts:")
                for table, count in verification['data_counts'].items():
                    print(f"  {table}: {count} rows")
            
            if verification['errors']:
                print(f"\nErrors: {len(verification['errors'])}")
                for error in verification['errors']:
                    print(f"  - {error}")
            
            return
        
        print("Starting database migration to multi-competitor schema...")
        print("This will create new normalized tables and migrate existing data.")
        
        response = input("Continue? (y/N): ")
        if response.lower() != 'y':
            logger.info("Migration cancelled by user")
            return
        
        # Step 1: Create new schema
        logger.info("Step 1: Creating new normalized schema...")
        await migration.create_new_schema()
        
        # Step 2: Populate default data
        logger.info("Step 2: Populating default competitors and platforms...")
        await migration.populate_default_data()
        
        # Step 3: Migrate existing data (if any)
        if not args.skip_migration:
            logger.info("Step 3: Migrating existing data...")
            migration_results = await migration.migrate_existing_data()
        else:
            migration_results = {}
        
        # Step 4: Create sample data (if requested)
        if args.create_samples:
            logger.info("Step 4: Creating sample data...")
            await migration.create_sample_data()
        
        # Step 5: Verify migration
        logger.info("Step 5: Verifying migration...")
        verification = await migration.verify_migration()
        
        # Summary
        print("\n" + "="*60)
        print("DATABASE MIGRATION COMPLETED")
        print("="*60)
        print(f"Migration Status: {'SUCCESS' if verification['success'] else 'FAILED'}")
        print(f"Tables Created: {len(verification['tables_created'])}")
        
        if migration_results:
            print("\nData Migration:")
            for key, count in migration_results.items():
                if count > 0:
                    print(f"  {key}: {count}")
        
        if verification['data_counts']:
            print("\nFinal Data Counts:")
            for table, count in verification['data_counts'].items():
                print(f"  {table}: {count} rows")
        
        if verification['errors']:
            print(f"\nErrors Encountered: {len(verification['errors'])}")
            for error in verification['errors']:
                print(f"  - {error}")
        
        print("\nNext Steps:")
        print("1. Run the cleanup script: python cleanup_supabase.py")
        print("2. Start the web UI: python web_ui/app.py")
        print("3. Start Celery workers: celery -A queue_system.celery_app worker")
        print("4. Run the orchestrator: python main_orchestrator.py pipeline")
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise
    
    finally:
        await migration.disconnect()

if __name__ == '__main__':
    asyncio.run(main())