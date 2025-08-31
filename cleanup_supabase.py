#!/usr/bin/env python3
"""
Supabase Table Cleanup Script
Removes old AIADMK-specific tables and prepares for multi-competitor schema
"""

import asyncio
import os
import sys
import logging
from typing import List, Dict
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.connection import DatabaseConnection

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SupabaseCleanup:
    """
    Handles cleanup of old Supabase tables
    """
    
    def __init__(self):
        self.db = DatabaseConnection()
        
        # Tables that should be removed (old AIADMK-specific structure)
        self.tables_to_remove = [
            'aiadmk_discovery',
            'aiadmk_engagement', 
            'aiadmk_sources',
            'aiadmk_keywords',
            'aiadmk_manual_queue',
            'aiadmk_analytics',
            'discovery_results',
            'engagement_results',
            'manual_submissions',
            'keyword_tracking',
            'source_monitoring',
            'analytics_data',
            'old_scraped_data',
            'temp_results',
            'legacy_data'
        ]
        
        # Views that might exist
        self.views_to_remove = [
            'aiadmk_summary',
            'engagement_summary', 
            'discovery_summary',
            'analytics_view'
        ]
        
        # Functions that might exist
        self.functions_to_remove = [
            'calculate_engagement_score',
            'update_analytics_summary',
            'process_discovery_results'
        ]
    
    async def connect(self):
        """Connect to database"""
        await self.db.connect()
        logger.info("Connected to Supabase database")
    
    async def disconnect(self):
        """Disconnect from database"""
        await self.db.close()
        logger.info("Disconnected from Supabase database")
    
    async def list_existing_tables(self) -> List[str]:
        """List all existing tables in the database"""
        query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
        ORDER BY table_name;
        """
        
        results = await self.db.execute_query(query)
        tables = [row['table_name'] for row in results]
        
        logger.info(f"Found {len(tables)} existing tables:")
        for table in tables:
            logger.info(f"  - {table}")
        
        return tables
    
    async def list_existing_views(self) -> List[str]:
        """List all existing views in the database"""
        query = """
        SELECT table_name 
        FROM information_schema.views 
        WHERE table_schema = 'public'
        ORDER BY table_name;
        """
        
        results = await self.db.execute_query(query)
        views = [row['table_name'] for row in results]
        
        logger.info(f"Found {len(views)} existing views:")
        for view in views:
            logger.info(f"  - {view}")
        
        return views
    
    async def backup_table_data(self, table_name: str, backup_dir: str = "backups") -> bool:
        """
        Create a backup of table data before deletion
        """
        try:
            os.makedirs(backup_dir, exist_ok=True)
            
            # Check if table exists and has data
            count_query = f"SELECT COUNT(*) as count FROM {table_name}"
            count_result = await self.db.execute_query(count_query)
            record_count = count_result[0]['count']
            
            if record_count == 0:
                logger.info(f"Table {table_name} is empty, skipping backup")
                return True
            
            # Export table data to JSON
            data_query = f"SELECT * FROM {table_name}"
            data = await self.db.execute_query(data_query)
            
            backup_file = os.path.join(backup_dir, f"{table_name}_backup.json")
            
            import json
            with open(backup_file, 'w') as f:
                json.dump([dict(row) for row in data], f, indent=2, default=str)
            
            logger.info(f"Backed up {record_count} records from {table_name} to {backup_file}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to backup table {table_name}: {e}")
            return False
    
    async def drop_table_safe(self, table_name: str, backup: bool = True) -> bool:
        """
        Safely drop a table with optional backup
        """
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
                logger.info(f"Table {table_name} does not exist, skipping")
                return True
            
            # Create backup if requested
            if backup:
                backup_success = await self.backup_table_data(table_name)
                if not backup_success:
                    logger.warning(f"Backup failed for {table_name}, proceeding with caution")
            
            # Drop the table
            drop_query = f"DROP TABLE IF EXISTS {table_name} CASCADE"
            await self.db.execute_query(drop_query)
            
            logger.info(f"Successfully dropped table: {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to drop table {table_name}: {e}")
            return False
    
    async def drop_view_safe(self, view_name: str) -> bool:
        """
        Safely drop a view
        """
        try:
            drop_query = f"DROP VIEW IF EXISTS {view_name} CASCADE"
            await self.db.execute_query(drop_query)
            
            logger.info(f"Successfully dropped view: {view_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to drop view {view_name}: {e}")
            return False
    
    async def drop_function_safe(self, function_name: str) -> bool:
        """
        Safely drop a function
        """
        try:
            drop_query = f"DROP FUNCTION IF EXISTS {function_name} CASCADE"
            await self.db.execute_query(drop_query)
            
            logger.info(f"Successfully dropped function: {function_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to drop function {function_name}: {e}")
            return False
    
    async def cleanup_all(self, backup: bool = True) -> Dict[str, any]:
        """
        Perform complete cleanup of old tables, views, and functions
        """
        results = {
            'tables_dropped': [],
            'tables_failed': [],
            'views_dropped': [],
            'views_failed': [],
            'functions_dropped': [],
            'functions_failed': [],
            'backup_created': backup
        }
        
        logger.info("Starting comprehensive Supabase cleanup...")
        
        # Get existing tables and views
        existing_tables = await self.list_existing_tables()
        existing_views = await self.list_existing_views()
        
        # Drop tables
        logger.info("Cleaning up tables...")
        for table_name in self.tables_to_remove:
            if table_name in existing_tables:
                success = await self.drop_table_safe(table_name, backup=backup)
                if success:
                    results['tables_dropped'].append(table_name)
                else:
                    results['tables_failed'].append(table_name)
        
        # Drop views
        logger.info("Cleaning up views...")
        for view_name in self.views_to_remove:
            if view_name in existing_views:
                success = await self.drop_view_safe(view_name)
                if success:
                    results['views_dropped'].append(view_name)
                else:
                    results['views_failed'].append(view_name)
        
        # Drop functions
        logger.info("Cleaning up functions...")
        for function_name in self.functions_to_remove:
            success = await self.drop_function_safe(function_name)
            if success:
                results['functions_dropped'].append(function_name)
            else:
                results['functions_failed'].append(function_name)
        
        # Clean up any remaining orphaned data
        await self.cleanup_orphaned_data()
        
        logger.info("Supabase cleanup completed!")
        logger.info(f"Tables dropped: {len(results['tables_dropped'])}")
        logger.info(f"Views dropped: {len(results['views_dropped'])}")
        logger.info(f"Functions dropped: {len(results['functions_dropped'])}")
        
        if results['tables_failed'] or results['views_failed'] or results['functions_failed']:
            logger.warning("Some cleanup operations failed:")
            if results['tables_failed']:
                logger.warning(f"Failed tables: {results['tables_failed']}")
            if results['views_failed']:
                logger.warning(f"Failed views: {results['views_failed']}")
            if results['functions_failed']:
                logger.warning(f"Failed functions: {results['functions_failed']}")
        
        return results
    
    async def cleanup_orphaned_data(self):
        """
        Clean up any orphaned sequences, indexes, or constraints
        """
        try:
            # Clean up sequences that might be left over
            sequences_query = """
            SELECT sequence_name 
            FROM information_schema.sequences 
            WHERE sequence_schema = 'public'
            AND sequence_name LIKE '%aiadmk%'
            OR sequence_name LIKE '%discovery%'
            OR sequence_name LIKE '%engagement%'
            """
            
            sequences = await self.db.execute_query(sequences_query)
            
            for seq in sequences:
                try:
                    await self.db.execute_query(f"DROP SEQUENCE IF EXISTS {seq['sequence_name']} CASCADE")
                    logger.info(f"Dropped orphaned sequence: {seq['sequence_name']}")
                except Exception as e:
                    logger.warning(f"Could not drop sequence {seq['sequence_name']}: {e}")
            
        except Exception as e:
            logger.warning(f"Error during orphaned data cleanup: {e}")

async def main():
    """Main cleanup function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Supabase Cleanup Tool')
    parser.add_argument('--no-backup', action='store_true', help='Skip backup creation')
    parser.add_argument('--list-only', action='store_true', help='Only list existing tables/views')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be deleted without actually deleting')
    
    args = parser.parse_args()
    
    cleanup = SupabaseCleanup()
    
    try:
        await cleanup.connect()
        
        if args.list_only:
            logger.info("=== EXISTING TABLES ===")
            await cleanup.list_existing_tables()
            logger.info("\n=== EXISTING VIEWS ===")
            await cleanup.list_existing_views()
            return
        
        if args.dry_run:
            logger.info("=== DRY RUN MODE ===")
            existing_tables = await cleanup.list_existing_tables()
            existing_views = await cleanup.list_existing_views()
            
            logger.info("\nTables that would be removed:")
            for table in cleanup.tables_to_remove:
                if table in existing_tables:
                    logger.info(f"  - {table} (EXISTS)")
                else:
                    logger.info(f"  - {table} (NOT FOUND)")
            
            logger.info("\nViews that would be removed:")
            for view in cleanup.views_to_remove:
                if view in existing_views:
                    logger.info(f"  - {view} (EXISTS)")
                else:
                    logger.info(f"  - {view} (NOT FOUND)")
            
            logger.info("\nFunctions that would be removed:")
            for func in cleanup.functions_to_remove:
                logger.info(f"  - {func}")
            
            return
        
        # Confirm before proceeding
        if not args.no_backup:
            response = input("This will cleanup old Supabase tables with backup. Continue? (y/N): ")
        else:
            response = input("This will cleanup old Supabase tables WITHOUT backup. Continue? (y/N): ")
        
        if response.lower() != 'y':
            logger.info("Cleanup cancelled by user")
            return
        
        # Perform cleanup
        results = await cleanup.cleanup_all(backup=not args.no_backup)
        
        print("\n" + "="*50)
        print("CLEANUP SUMMARY")
        print("="*50)
        print(f"Tables dropped: {len(results['tables_dropped'])}")
        print(f"Views dropped: {len(results['views_dropped'])}")
        print(f"Functions dropped: {len(results['functions_dropped'])}")
        print(f"Backup created: {results['backup_created']}")
        
        if results['tables_failed'] or results['views_failed'] or results['functions_failed']:
            print(f"\nFailed operations: {len(results['tables_failed']) + len(results['views_failed']) + len(results['functions_failed'])}")
        
        print("\nCleanup completed successfully!")
        
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        raise
    
    finally:
        await cleanup.disconnect()

if __name__ == '__main__':
    asyncio.run(main())