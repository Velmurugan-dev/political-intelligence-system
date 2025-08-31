#!/usr/bin/env python3
"""
AIADMK Political Intelligence System - Command Line Interface
Production-ready CLI for the complete AIADMK intelligence system
"""

import asyncio
import argparse
import sys
import logging
import json
from datetime import datetime

from aiadmk_intelligence_engine import get_intelligence_engine
from config import get_config, validate_system_config

def setup_logging(verbose: bool = False):
    """Setup logging configuration"""
    level = logging.DEBUG if verbose else logging.INFO
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )

async def run_single_cycle():
    """Run a single intelligence cycle"""
    print("🏛️ AIADMK Political Intelligence System - Single Cycle Mode")
    print("=" * 60)
    
    engine = get_intelligence_engine()
    
    if not await engine.initialize():
        print("❌ System initialization failed")
        return False
    
    try:
        result = await engine.run_intelligence_cycle()
        
        print("\n📊 CYCLE RESULTS:")
        print("=" * 60)
        print(f"Success: {'✅ YES' if result['success'] else '❌ NO'}")
        print(f"Duration: {result['duration_seconds']:.1f} seconds")
        print(f"Content Discovered: {result['total_content_discovered']}")
        print(f"Content Processed: {result['total_content_processed']}")
        
        if result['errors']:
            print(f"Errors: {len(result['errors'])}")
            for error in result['errors'][:5]:  # Show first 5 errors
                print(f"  - {error}")
        
        # Platform breakdown
        if 'scraping' in result and 'platform_results' in result['scraping']:
            print(f"\n📱 PLATFORM RESULTS:")
            for platform, platform_result in result['scraping']['platform_results'].items():
                if platform_result.get('success'):
                    scraping = platform_result.get('scraping', {})
                    processing = platform_result.get('processing', {})
                    content_count = (
                        scraping.get('aiadmk_posts', 0) or
                        scraping.get('aiadmk_videos', 0) or  
                        scraping.get('aiadmk_tweets', 0) or
                        scraping.get('aiadmk_articles', 0) or
                        processing.get('aiadmk_articles', 0) or 0
                    )
                    print(f"  {platform.upper()}: ✅ {content_count} items")
                else:
                    print(f"  {platform.upper()}: ❌ Failed")
        
        return result['success']
        
    except Exception as e:
        print(f"❌ Single cycle failed: {e}")
        return False
    finally:
        await engine.shutdown()

async def run_continuous_monitoring(interval_minutes: int):
    """Run continuous monitoring"""
    print(f"🏛️ AIADMK Political Intelligence System - Continuous Mode ({interval_minutes}min cycles)")
    print("=" * 60)
    print("Press Ctrl+C to stop monitoring")
    
    engine = get_intelligence_engine()
    
    if not await engine.initialize():
        print("❌ System initialization failed")
        return False
    
    try:
        await engine.run_continuous_monitoring(cycle_interval_minutes=interval_minutes)
        return True
    except KeyboardInterrupt:
        print("\n🛑 Monitoring stopped by user")
        return True
    except Exception as e:
        print(f"❌ Continuous monitoring failed: {e}")
        return False
    finally:
        await engine.shutdown()

async def show_system_status():
    """Show system status and statistics"""
    print("🏛️ AIADMK Political Intelligence System - Status")
    print("=" * 60)
    
    engine = get_intelligence_engine()
    
    if not await engine.initialize():
        print("❌ System initialization failed")
        return False
    
    try:
        status = await engine.get_system_status()
        
        print("🖥️  SYSTEM INFO:")
        system_info = status['system_info']
        print(f"   Running: {system_info['running']}")
        print(f"   Total Runs: {system_info['total_runs']}")
        print(f"   Success Rate: {system_info['success_rate']}")
        if system_info['uptime']:
            print(f"   Last Run: {system_info['uptime']}")
        
        print(f"\n📊 CONTENT STATS:")
        content_stats = status['content_stats']
        print(f"   Total Discovered: {content_stats['total_discovered']}")
        print(f"   Total Processed: {content_stats['total_processed']}")
        
        db_stats = content_stats.get('database_stats', {})
        if db_stats:
            print(f"   Database Posts: {db_stats.get('total_posts', 0)}")
            for platform, stats in db_stats.get('platforms', {}).items():
                print(f"     {platform.upper()}: {stats.get('posts', 0)} posts")
        
        print(f"\n📱 PLATFORM STATS:")
        for platform, stats in status['platform_stats'].items():
            print(f"   {platform.upper()}: {stats['runs']} runs, {stats['content']} items, {stats['errors']} errors")
        
        print(f"\n⚙️  CONFIGURATION:")
        config = status['configuration']
        print(f"   Enabled Platforms: {len(config['enabled_platforms'])}")
        print(f"   Max Concurrent Scrapers: {config['max_concurrent_scrapers']}")
        print(f"   Batch Size: {config['batch_size']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Status check failed: {e}")
        return False
    finally:
        await engine.shutdown()

async def test_configuration():
    """Test system configuration"""
    print("🏛️ AIADMK Political Intelligence System - Configuration Test")
    print("=" * 60)
    
    config = get_config()
    
    print("🔧 Testing configuration...")
    is_valid = validate_system_config()
    
    if is_valid:
        print("✅ Configuration is valid")
        
        # Show enabled platforms
        enabled_platforms = config.get_enabled_platforms()
        print(f"\n📱 Enabled platforms ({len(enabled_platforms)}):")
        for platform in enabled_platforms:
            print(f"   - {platform}")
        
        # Show API status
        print(f"\n🔑 API Status:")
        apis = ['serpapi', 'brave', 'firecrawl', 'apify']
        for api in apis:
            available = config.is_api_available(api)
            status = "✅ Available" if available else "❌ Not configured"
            print(f"   {api.upper()}: {status}")
        
        return True
    else:
        print("❌ Configuration validation failed")
        return False

def main():
    """Main CLI function"""
    parser = argparse.ArgumentParser(
        description="AIADMK Political Intelligence System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --single-cycle           # Run one intelligence cycle
  %(prog)s --continuous             # Run continuous monitoring (30min cycles)  
  %(prog)s --continuous --interval 60  # Run continuous monitoring (60min cycles)
  %(prog)s --status                 # Show system status
  %(prog)s --test-config            # Test system configuration
        """
    )
    
    parser.add_argument('--single-cycle', action='store_true',
                       help='Run a single intelligence cycle')
    parser.add_argument('--continuous', action='store_true',
                       help='Run continuous monitoring')
    parser.add_argument('--interval', type=int, default=30,
                       help='Interval between cycles in minutes (default: 30)')
    parser.add_argument('--status', action='store_true',
                       help='Show system status and statistics')
    parser.add_argument('--test-config', action='store_true',
                       help='Test system configuration')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.verbose)
    
    # Determine action
    if args.test_config:
        success = asyncio.run(test_configuration())
    elif args.status:
        success = asyncio.run(show_system_status())
    elif args.single_cycle:
        success = asyncio.run(run_single_cycle())
    elif args.continuous:
        success = asyncio.run(run_continuous_monitoring(args.interval))
    else:
        parser.print_help()
        return 0
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())