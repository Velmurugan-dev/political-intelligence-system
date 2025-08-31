#!/usr/bin/env python3
"""
Database module for Political Intelligence System
Provides database connection and management functionality
"""

# Import everything from the root database.py and the connection module
import sys
import os
import importlib.util

# Import from the root database.py file
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
spec = importlib.util.spec_from_file_location("root_database", os.path.join(parent_dir, "database.py"))
root_database = importlib.util.module_from_spec(spec)
spec.loader.exec_module(root_database)

# Import the DatabaseConnection alias from connection module
from .connection import DatabaseConnection

# Export all classes and functions from root database module
DatabaseManager = root_database.DatabaseManager
get_database = root_database.get_database
close_database = root_database.close_database
test_database_connection = root_database.test_database_connection

__all__ = ['DatabaseConnection', 'DatabaseManager', 'get_database', 'close_database', 'test_database_connection']