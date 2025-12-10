#!/usr/bin/env python3
"""
Add rt_slug column to movies table without losing existing data.
This is a surgical database migration that only adds one new column.

Usage:
    python add_rt_slug_column.py
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / "src"))

from sqlalchemy import create_engine, text
from database.db import DEFAULT_DB_PATH

def add_rt_slug_column():
    """Add rt_slug column to movies table if it doesn't exist."""
    
    # Get database path
    db_path = DEFAULT_DB_PATH
    print(f" Database: {db_path}")
    
    # Create engine
    engine = create_engine(f'sqlite:///{db_path}')
    
    # Check if column already exists
    with engine.connect() as conn:
        # Get table info
        result = conn.execute(text("PRAGMA table_info(movies)"))
        columns = [row[1] for row in result]
        
        if 'rt_slug' in columns:
            print(" Column 'rt_slug' already exists in movies table!")
            return
        
        print(" Adding 'rt_slug' column to movies table...")
        
        # Add the column (SQLite allows adding columns without recreating table)
        conn.execute(text("ALTER TABLE movies ADD COLUMN rt_slug VARCHAR(200)"))
        conn.commit()
        
        print(" Successfully added 'rt_slug' column!")
        
        # Verify the column was added
        result = conn.execute(text("PRAGMA table_info(movies)"))
        columns = [row[1] for row in result]
        
        if 'rt_slug' in columns:
            print(" Verification successful: 'rt_slug' column is now in the table")
            
            # Show column details
            result = conn.execute(text("PRAGMA table_info(movies)"))
            for row in result:
                if row[1] == 'rt_slug':
                    print(f"\n Column details:")
                    print(f"   Name: {row[1]}")
                    print(f"   Type: {row[2]}")
                    print(f"   Not Null: {bool(row[3])}")
                    print(f"   Default: {row[4]}")
                    break
        else:
            print(" Error: Column was not added successfully")
            return
    
    print("\n" + "="*80)
    print(" MIGRATION COMPLETE!")
    print("="*80)
    print("The 'rt_slug' column has been added to the movies table.")
    print("All existing data has been preserved.")
    print("\nYou can now:")
    print("1. Update src/database/models.py to add: rt_slug = Column(String(200), nullable=True)")
    print("2. Use the rt_slug field in your notebook cells")
    print("="*80)

if __name__ == "__main__":
    try:
        add_rt_slug_column()
    except Exception as e:
        print(f"\n Error during migration: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
