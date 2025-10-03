#!/usr/bin/env python3
"""
Versioning

...or how to handle versioned and non-versioned tables
gracefully without system failures.

Key Principles:
1. Non-versioned tables don't break the system
2. Graceful degradation with warnings
3. Easy to add versioning later
"""

import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

# Setup logging
logger = logging.getLogger(__name__)

@dataclass
class VersionInfo:
    """Version information for a table"""
    version_id: Optional[int]
    version_number: Optional[str]
    version_family: Optional[str]
    is_versioned: bool

class VersioningManager:
    """Manages versioned and non-versioned tables gracefully"""
    
    def __init__(self, db_connection):
        self.db = db_connection
        self._versioned_tables_cache = None
        self._load_versioned_tables()
    
    def _load_versioned_tables(self):
        """Load list of versioned tables from domain_family_map"""
        try:
            query = """
            SELECT dfm.table_name, vf.short_code as version_family
            FROM domain_family_map dfm
            JOIN version_family vf ON dfm.version_family_id = vf.id
            WHERE vf.is_active = true
            """
            result = self.db.execute(query).fetchall()
            self._versioned_tables_cache = {
                row['table_name']: row['version_family'] 
                for row in result
            }
            logger.info(f"Loaded {len(self._versioned_tables_cache)} versioned tables")
        except Exception as e:
            logger.warning(f"Could not load versioned tables: {e}")
            self._versioned_tables_cache = {}
    
    def is_table_versioned(self, table_name: str) -> bool:
        """Check if table participates in versioning system"""
        return table_name in self._versioned_tables_cache
    
    def get_version_info(self, table_name: str) -> VersionInfo:
        """Get version information for a table (graceful)"""
        if not self.is_table_versioned(table_name):
            return VersionInfo(
                version_id=None,
                version_number=None,
                version_family=None,
                is_versioned=False
            )
        
        try:
            version_family = self._versioned_tables_cache[table_name]
            query = """
            SELECT v.id, v.version_number, vf.short_code
            FROM version v
            JOIN version_family vf ON v.version_family_id = vf.id
            WHERE vf.short_code = %s AND v.is_active = true
            """
            result = self.db.execute(query, (version_family,)).fetchone()
            
            if result:
                return VersionInfo(
                    version_id=result['id'],
                    version_number=result['version_number'],
                    version_family=result['short_code'],
                    is_versioned=True
                )
            else:
                logger.warning(f"No active version found for {table_name}")
                return VersionInfo(None, None, version_family, True)
                
        except Exception as e:
            logger.warning(f"Error getting version info for {table_name}: {e}")
            return VersionInfo(None, None, None, True)
    
    def get_table_data(self, table_name: str, version_family: Optional[str] = None) -> List[Dict]:
        """Get table data with automatic version handling"""
        version_info = self.get_version_info(table_name)
        
        if not version_info.is_versioned:
            # Non-versioned table - simple query
            logger.debug(f"Fetching non-versioned table: {table_name}")
            return self._fetch_table_data(table_name)
        
        if version_info.version_id is None:
            logger.warning(f"No active version for {table_name}, fetching without version filter")
            return self._fetch_table_data(table_name)
        
        # Versioned table - include version filter
        logger.debug(f"Fetching versioned table: {table_name} (v{version_info.version_number})")
        return self._fetch_versioned_table_data(table_name, version_info)
    
    def _fetch_table_data(self, table_name: str) -> List[Dict]:
        """Fetch data from non-versioned table"""
        try:
            query = f"SELECT * FROM {table_name} WHERE is_active = true"
            return self.db.execute(query).fetchall()
        except Exception as e:
            # Fallback if no is_active column
            logger.debug(f"No is_active column in {table_name}, fetching all data")
            query = f"SELECT * FROM {table_name}"
            return self.db.execute(query).fetchall()
    
    def _fetch_versioned_table_data(self, table_name: str, version_info: VersionInfo) -> List[Dict]:
        """Fetch data from versioned table"""
        # Determine version field name based on table
        version_field = self._get_version_field_name(table_name, version_info.version_family)
        
        query = f"""
        SELECT * FROM {table_name} 
        WHERE {version_field} = %s 
        AND is_active = true
        """
        return self.db.execute(query, (version_info.version_id,)).fetchall()
    
    def _get_version_field_name(self, table_name: str, version_family: str) -> str:
        """Determine the version field name for a table"""
        # Common patterns for version field names
        field_mappings = {
            'theme': 'theme_version_id',
            'scenario': 'scenario_version_id',
            'assumption': 'assumption_version_id',
            'operation': 'operation_version_id',
            'outcome_framework': 'metrics_version_id',
            'calsim_variable': 'variable_version_id',
            'hydroclimate': 'hydroclimate_version_id',
            'spatial_data': 'geometries_version_id',
            'interpretive': 'interpretive_version_id',
            'metadata': 'metadata_version_id'
        }
        
        return field_mappings.get(version_family, f'{version_family}_version_id')

class TableManager:
    """High-level interface for table operations"""
    
    def __init__(self, db_connection):
        self.versioning = VersioningManager(db_connection)
    
    def get_themes(self) -> List[Dict]:
        """Get themes (versioned table)"""
        return self.versioning.get_table_data('theme')
    
    def get_users(self) -> List[Dict]:
        """Get users (non-versioned table)"""
        return self.versioning.get_table_data('user')
    
    def get_hydrologic_regions(self) -> List[Dict]:
        """Get hydrologic regions (non-versioned table)"""
        return self.versioning.get_table_data('hydrologic_region')
    
    def get_scenarios(self, version_family: str = 'scenario') -> List[Dict]:
        """Get scenarios (versioned table)"""
        return self.versioning.get_table_data('scenario', version_family)
    
    def get_any_table(self, table_name: str) -> List[Dict]:
        """Get any table with automatic version handling"""
        return self.versioning.get_table_data(table_name)

# Example usage and testing
def example_usage():
    """Demonstrate graceful mixed table handling"""
    
    # Mock database connection for example
    class MockDB:
        def execute(self, query, params=None):
            class MockResult:
                def fetchall(self):
                    if 'domain_family_map' in query:
                        return [
                            {'table_name': 'theme', 'version_family': 'theme'},
                            {'table_name': 'scenario', 'version_family': 'scenario'},
                        ]
                    elif 'version' in query and 'theme' in str(params or []):
                        return [{'id': 1, 'version_number': '1.0.0', 'short_code': 'theme'}]
                    else:
                        return []
                def fetchone(self):
                    return self.fetchall()[0] if self.fetchall() else None
            return MockResult()
    
    # Initialize system
    db = MockDB()
    table_manager = TableManager(db)
    
    # Examples of mixed table access
    examples = [
        ('user', 'Non-versioned infrastructure table'),
        ('hydrologic_region', 'Non-versioned lookup table'),  
        ('theme', 'Versioned research domain table'),
        ('scenario', 'Versioned research domain table'),
        ('nonexistent_table', 'Table that might not exist'),
    ]
    
    for table_name, description in examples:
        print(f"\n--- {description} ---")
        try:
            version_info = table_manager.versioning.get_version_info(table_name)
            print(f"Table: {table_name}")
            print(f"Versioned: {version_info.is_versioned}")
            print(f"Version: {version_info.version_number or 'N/A'}")
            
            # This would work regardless of versioning status
            # data = table_manager.get_any_table(table_name)
            # print(f"Records: {len(data)}")
            
        except Exception as e:
            print(f"Error (gracefully handled): {e}")

if __name__ == '__main__':
    example_usage() 