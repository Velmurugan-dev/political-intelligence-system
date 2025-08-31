#!/usr/bin/env python3
"""
Database connection module for Political Intelligence System
Aliases the main DatabaseManager class as DatabaseConnection for test compatibility
"""

import sys
import os

# Add parent directory to path to import the main database module
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)

# Import directly from the root database.py file to avoid circular imports
import importlib.util
spec = importlib.util.spec_from_file_location("root_database", os.path.join(parent_dir, "database.py"))
root_database = importlib.util.module_from_spec(spec)
spec.loader.exec_module(root_database)

# Alias DatabaseManager as DatabaseConnection for test compatibility
DatabaseConnection = root_database.DatabaseManager

# Also export the utility functions
get_database = root_database.get_database
close_database = root_database.close_database
test_database_connection = root_database.test_database_connection

__all__ = ['DatabaseConnection', 'get_database', 'close_database', 'test_database_connection']