"""
SQLite Source Connector
Handles data extraction from SQLite databases
"""
import sqlite3
from typing import Iterator, Dict, Any, List, Optional
from pathlib import Path
import logging

from app.connectors.base import (
    SourceConnector,
    ConnectionTestResult,
    Schema,
    Table,
    Column,
    Record,
    State,
    DataType
)

logger = logging.getLogger(__name__)


class SQLiteSource(SourceConnector):
    """
    SQLite source connector
    Supports full refresh and incremental syncs
    """
    
    # Map SQLite types to standard DataType enum
    TYPE_MAPPING = {
        'INTEGER': DataType.INTEGER,
        'INT': DataType.INTEGER,
        'TINYINT': DataType.INTEGER,
        'SMALLINT': DataType.INTEGER,
        'MEDIUMINT': DataType.INTEGER,
        'BIGINT': DataType.INTEGER,
        'REAL': DataType.FLOAT,
        'DOUBLE': DataType.FLOAT,
        'FLOAT': DataType.FLOAT,
        'NUMERIC': DataType.FLOAT,
        'DECIMAL': DataType.FLOAT,
        'TEXT': DataType.STRING,
        'VARCHAR': DataType.STRING,
        'CHAR': DataType.STRING,
        'CLOB': DataType.STRING,
        'BLOB': DataType.BINARY,
        'BOOLEAN': DataType.BOOLEAN,
        'DATE': DataType.DATE,
        'DATETIME': DataType.DATETIME,
        'TIMESTAMP': DataType.DATETIME,
    }
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize SQLite source connector
        
        Config format:
        {
            "database_path": "/path/to/database.db",  # or ":memory:"
            "batch_size": 1000  # Optional
        }
        """
        super().__init__(config)
        self.database_path = config['database_path']
        self._batch_size = config.get('batch_size', 1000)
    
    def connect(self) -> None:
        """Establish connection to SQLite"""
        if self._connection is None:
            try:
                # Validate database exists (unless in-memory)
                if self.database_path != ":memory:":
                    db_path = Path(self.database_path)
                    if not db_path.exists():
                        raise FileNotFoundError(f"Database file not found: {self.database_path}")
                
                self._connection = sqlite3.connect(
                    self.database_path,
                    check_same_thread=False
                )
                self._connection.row_factory = sqlite3.Row
                logger.info(f"Connected to SQLite: {self.database_path}")
            except Exception as e:
                logger.error(f"Failed to connect to SQLite: {str(e)}")
                raise
    
    def disconnect(self) -> None:
        """Close SQLite connection"""
        if self._connection:
            try:
                self._connection.close()
                logger.info("SQLite connection closed")
            except Exception as e:
                logger.warning(f"Error closing connection: {str(e)}")
            finally:
                self._connection = None
    
    def test_connection(self) -> ConnectionTestResult:
        """Test SQLite connection"""
        try:
            self.connect()
            cursor = self._connection.cursor()
            cursor.execute("SELECT sqlite_version();")
            version = cursor.fetchone()[0]
            cursor.close()
            
            return ConnectionTestResult(
                success=True,
                message=f"Connected successfully",
                metadata={"version": f"SQLite {version}"}
            )
        except FileNotFoundError as e:
            return ConnectionTestResult(
                success=False,
                message=str(e)
            )
        except Exception as e:
            return ConnectionTestResult(
                success=False,
                message=f"Connection failed: {str(e)}"
            )
        finally:
            self.disconnect()
    
    def discover_schema(self) -> Schema:
        """
        Discover schema from SQLite database
        Returns tables, columns, and constraints
        """
        self.connect()
        cursor = self._connection.cursor()
        
        tables = []
        
        try:
            # Get all tables (exclude system tables)
            cursor.execute("""
                SELECT name 
                FROM sqlite_master 
                WHERE type='table' 
                  AND name NOT LIKE 'sqlite_%'
                ORDER BY name;
            """)
            
            table_names = [row[0] for row in cursor.fetchall()]
            
            for table_name in table_names:
                # Get table info
                cursor.execute(f"PRAGMA table_info('{table_name}');")
                column_rows = cursor.fetchall()
                
                # Get foreign keys
                cursor.execute(f"PRAGMA foreign_key_list('{table_name}');")
                fk_rows = cursor.fetchall()
                
                foreign_keys = {}
                for fk_row in fk_rows:
                    # fk_row: (id, seq, table, from, to, on_update, on_delete, match)
                    from_col = fk_row[3]
                    to_table = fk_row[2]
                    to_col = fk_row[4]
                    foreign_keys[from_col] = f"{to_table}.{to_col}"
                
                # Build columns
                columns = []
                for col_row in column_rows:
                    # col_row: (cid, name, type, notnull, dflt_value, pk)
                    col_name = col_row[1]
                    col_type = col_row[2].upper().split('(')[0]  # Remove size info
                    
                    data_type = self.TYPE_MAPPING.get(col_type, DataType.STRING)
                    
                    columns.append(Column(
                        name=col_name,
                        data_type=data_type,
                        nullable=(col_row[3] == 0),
                        primary_key=(col_row[5] > 0),
                        foreign_key=foreign_keys.get(col_name),
                        default_value=col_row[4]
                    ))
                
                # Get row count
                cursor.execute(f"SELECT COUNT(*) FROM '{table_name}';")
                row_count = cursor.fetchone()[0]
                
                tables.append(Table(
                    name=table_name,
                    columns=columns,
                    row_count=row_count
                ))
            
            cursor.close()
            
            # Get SQLite version
            cursor = self._connection.cursor()
            cursor.execute("SELECT sqlite_version();")
            version = f"SQLite {cursor.fetchone()[0]}"
            cursor.close()
            
            return Schema(
                tables=tables,
                version=version
            )
            
        except Exception as e:
            logger.error(f"Schema discovery failed: {str(e)}")
            raise
        finally:
            self.disconnect()
    
    def read(
        self,
        stream: str,
        state: Optional[State] = None,
        query: Optional[str] = None
    ) -> Iterator[Record]:
        """
        Read data from SQLite table
        
        Args:
            stream: Table name to read from
            state: Optional state for incremental sync
            query: Optional custom SQL query
            
        Yields:
            Record objects with data
        """
        self.connect()
        cursor = self._connection.cursor()
        
        try:
            # Build query
            if query:
                # Use custom query
                sql = query
            elif state and state.cursor_field:
                # Incremental sync
                sql = f"""
                    SELECT * FROM "{stream}"
                    WHERE "{state.cursor_field}" > ?
                    ORDER BY "{state.cursor_field}"
                """
                cursor.execute(sql, (state.cursor_value,))
            else:
                # Full refresh
                sql = f'SELECT * FROM "{stream}"'
                cursor.execute(sql)
            
            if not query and not (state and state.cursor_field):
                cursor.execute(sql)
            
            # Fetch in batches
            while True:
                rows = cursor.fetchmany(self._batch_size)
                if not rows:
                    break
                
                for row in rows:
                    yield Record(
                        stream=stream,
                        data=dict(row)
                    )
            
            cursor.close()
            
        except Exception as e:
            logger.error(f"Failed to read from {stream}: {str(e)}")
            raise
        finally:
            self.disconnect()
    
    def get_record_count(self, stream: str) -> int:
        """Get exact record count for a table"""
        self.connect()
        cursor = self._connection.cursor()
        
        try:
            cursor.execute(f'SELECT COUNT(*) FROM "{stream}"')
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except Exception as e:
            logger.error(f"Failed to get record count for {stream}: {str(e)}")
            raise
        finally:
            self.disconnect()