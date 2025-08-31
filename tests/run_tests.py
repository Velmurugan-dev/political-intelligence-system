#!/usr/bin/env python3
"""
Test Runner for Political Intelligence System
Runs the complete test suite with proper configuration and reporting
"""

import os
import sys
import pytest
import argparse
import asyncio
from datetime import datetime

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

def main():
    """Main test runner function"""
    parser = argparse.ArgumentParser(description='Political Intelligence System Test Runner')
    
    # Test selection arguments
    parser.add_argument('--unit', action='store_true', help='Run unit tests only')
    parser.add_argument('--integration', action='store_true', help='Run integration tests only')
    parser.add_argument('--performance', action='store_true', help='Run performance tests only')
    parser.add_argument('--all', action='store_true', help='Run all tests (default)')
    
    # Test configuration arguments
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    parser.add_argument('--coverage', action='store_true', help='Generate coverage report')
    parser.add_argument('--html-cov', action='store_true', help='Generate HTML coverage report')
    parser.add_argument('--parallel', '-n', type=int, default=1, help='Number of parallel workers')
    
    # Test filtering arguments
    parser.add_argument('--module', help='Run specific test module (e.g., test_discovery_engine)')
    parser.add_argument('--test', '-k', help='Run specific test pattern')
    parser.add_argument('--markers', '-m', help='Run tests with specific markers')
    
    # Environment arguments
    parser.add_argument('--test-db', help='Test database URL')
    parser.add_argument('--real-apis', action='store_true', help='Enable real API integration tests')
    
    # Output arguments
    parser.add_argument('--junit-xml', help='Generate JUnit XML report')
    parser.add_argument('--html', help='Generate HTML test report')
    
    args = parser.parse_args()
    
    # Set up environment
    setup_test_environment(args)
    
    # Build pytest arguments
    pytest_args = build_pytest_args(args)
    
    # Run tests
    print(f"🧪 Starting Political Intelligence System Tests")
    print(f"📅 Test run started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🔧 Pytest arguments: {' '.join(pytest_args)}")
    print("=" * 80)
    
    start_time = datetime.now()
    exit_code = pytest.main(pytest_args)
    end_time = datetime.now()
    
    print("=" * 80)
    print(f"⏱️  Test run completed in: {end_time - start_time}")
    print(f"📊 Exit code: {exit_code}")
    
    if exit_code == 0:
        print("✅ All tests passed!")
    else:
        print("❌ Some tests failed!")
    
    # Generate summary report
    generate_summary_report(args, exit_code, end_time - start_time)
    
    return exit_code

def setup_test_environment(args):
    """Set up test environment variables"""
    # Set test environment
    os.environ['TESTING'] = '1'
    os.environ['ENVIRONMENT'] = 'test'
    
    # Database configuration
    if args.test_db:
        os.environ['TEST_DATABASE_URL'] = args.test_db
    elif not os.getenv('TEST_DATABASE_URL'):
        os.environ['TEST_DATABASE_URL'] = 'postgresql://test_user:test_pass@localhost/test_political_intelligence'
    
    # Integration test configuration
    if args.real_apis or args.integration:
        os.environ['RUN_INTEGRATION_TESTS'] = '1'
    
    # Performance test configuration
    if args.performance:
        os.environ['RUN_PERFORMANCE_TESTS'] = '1'
    
    # API keys (use test keys or mock)
    if not os.getenv('SERPAPI_KEY'):
        os.environ['SERPAPI_KEY'] = 'test_serpapi_key'
    if not os.getenv('BRAVE_API_KEY'):
        os.environ['BRAVE_API_KEY'] = 'test_brave_key'
    if not os.getenv('APIFY_API_TOKEN'):
        os.environ['APIFY_API_TOKEN'] = 'test_apify_token'
    if not os.getenv('FIRECRAWL_API_KEY'):
        os.environ['FIRECRAWL_API_KEY'] = 'test_firecrawl_key'

def build_pytest_args(args):
    """Build pytest command line arguments"""
    pytest_args = []
    
    # Add test directory
    test_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Test selection
    if args.unit:
        pytest_args.extend([
            f"{test_dir}/test_discovery_engine.py",
            f"{test_dir}/test_engagement_engine.py",
            f"{test_dir}/test_deduplication_engine.py",
            f"{test_dir}/test_main_orchestrator.py",
            f"{test_dir}/test_web_ui.py"
        ])
    elif args.integration:
        pytest_args.append(f"{test_dir}/test_integration.py")
    elif args.performance:
        pytest_args.extend(["-m", "performance"])
    elif args.module:
        pytest_args.append(f"{test_dir}/{args.module}.py")
    else:
        # Run all tests by default
        pytest_args.append(test_dir)
    
    # Verbosity
    if args.verbose:
        pytest_args.append("-v")
    else:
        pytest_args.append("-q")
    
    # Coverage
    if args.coverage:
        pytest_args.extend([
            "--cov=engines",
            "--cov=database",
            "--cov=queue_system",
            "--cov=web_ui",
            "--cov=main_orchestrator",
            "--cov-report=term-missing"
        ])
        
        if args.html_cov:
            pytest_args.extend([
                "--cov-report=html:htmlcov"
            ])
    
    # Parallel execution
    if args.parallel > 1:
        pytest_args.extend(["-n", str(args.parallel)])
    
    # Test filtering
    if args.test:
        pytest_args.extend(["-k", args.test])
    
    if args.markers:
        pytest_args.extend(["-m", args.markers])
    
    # Output formats
    if args.junit_xml:
        pytest_args.extend([f"--junit-xml={args.junit_xml}"])
    
    if args.html:
        pytest_args.extend([f"--html={args.html}", "--self-contained-html"])
    
    # Additional pytest options
    pytest_args.extend([
        "--tb=short",  # Shorter traceback format
        "--strict-markers",  # Strict marker checking
        "--disable-warnings"  # Disable warnings for cleaner output
    ])
    
    return pytest_args

def generate_summary_report(args, exit_code, duration):
    """Generate a summary report of the test run"""
    report_file = "test_summary.txt"
    
    with open(report_file, 'w') as f:
        f.write("Political Intelligence System - Test Summary Report\n")
        f.write("=" * 60 + "\n\n")
        
        f.write(f"Test Run Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Duration: {duration}\n")
        f.write(f"Exit Code: {exit_code}\n")
        f.write(f"Result: {'PASSED' if exit_code == 0 else 'FAILED'}\n\n")
        
        f.write("Test Configuration:\n")
        f.write(f"  - Unit Tests: {'Yes' if args.unit else 'No'}\n")
        f.write(f"  - Integration Tests: {'Yes' if args.integration else 'No'}\n")
        f.write(f"  - Performance Tests: {'Yes' if args.performance else 'No'}\n")
        f.write(f"  - Coverage: {'Yes' if args.coverage else 'No'}\n")
        f.write(f"  - Parallel Workers: {args.parallel}\n")
        f.write(f"  - Real APIs: {'Yes' if args.real_apis else 'No'}\n\n")
        
        if args.coverage:
            f.write("Coverage reports generated:\n")
            f.write("  - Terminal: Yes\n")
            if args.html_cov:
                f.write("  - HTML: htmlcov/index.html\n")
        
        f.write("\nEnvironment Variables:\n")
        test_env_vars = [
            'TEST_DATABASE_URL', 'RUN_INTEGRATION_TESTS', 'RUN_PERFORMANCE_TESTS',
            'SERPAPI_KEY', 'BRAVE_API_KEY', 'APIFY_API_TOKEN', 'FIRECRAWL_API_KEY'
        ]
        
        for var in test_env_vars:
            value = os.getenv(var, 'Not set')
            if 'KEY' in var or 'TOKEN' in var:
                value = value[:8] + "***" if value != 'Not set' else 'Not set'
            f.write(f"  - {var}: {value}\n")
    
    print(f"📄 Test summary report saved to: {report_file}")

def run_quick_tests():
    """Run a quick subset of tests for development"""
    pytest_args = [
        os.path.dirname(os.path.abspath(__file__)),
        "-v",
        "-x",  # Stop on first failure
        "--tb=short",
        "-k", "not integration and not performance"
    ]
    
    print("🚀 Running quick test suite...")
    return pytest.main(pytest_args)

def run_ci_tests():
    """Run tests suitable for CI/CD pipeline"""
    pytest_args = [
        os.path.dirname(os.path.abspath(__file__)),
        "-v",
        "--cov=engines",
        "--cov=database", 
        "--cov=queue_system",
        "--cov=web_ui",
        "--cov=main_orchestrator",
        "--cov-report=xml",
        "--cov-report=term",
        "--junit-xml=test-results.xml",
        "--tb=short",
        "--strict-markers"
    ]
    
    print("🔄 Running CI test suite...")
    return pytest.main(pytest_args)

if __name__ == '__main__':
    # Check for special commands
    if len(sys.argv) > 1:
        if sys.argv[1] == 'quick':
            exit_code = run_quick_tests()
            sys.exit(exit_code)
        elif sys.argv[1] == 'ci':
            exit_code = run_ci_tests()
            sys.exit(exit_code)
    
    # Run normal test suite
    exit_code = main()
    sys.exit(exit_code)