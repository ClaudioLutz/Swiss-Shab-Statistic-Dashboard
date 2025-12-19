# %%
"""
Utility module for safe parquet operations.
Handles PyArrow extension type registration issues that occur with Python 3.13 + pandas 2.3.3 + pyarrow 22.x
"""

import os
import pandas as pd
import pyarrow as pa
import logging

# Must be set before any pandas/pyarrow imports in the main modules
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

logger = logging.getLogger(__name__)

# Track if we've already attempted to handle the extension types
_extension_types_handled = False


def handle_extension_type_registration():
    """
    Handle PyArrow extension type registration issues.
    This prevents the "A type extension with name pandas.period already defined" error.
    """
    global _extension_types_handled
    
    if _extension_types_handled:
        return
    
    try:
        # Try to unregister pandas extension types if they're already registered
        # This allows them to be re-registered without error
        from pyarrow.pandas_compat import _register_pandas_extension_types
        
        for ext_type in ['pandas.period', 'pandas.interval', 'pandas.categorical']:
            try:
                # Unregister if exists (PyArrow >= 4.0)
                if hasattr(pa, 'unregister_extension_type'):
                    pa.unregister_extension_type(ext_type)
                    logger.debug(f"Unregistered extension type: {ext_type}")
            except (KeyError, pa.ArrowInvalid, pa.ArrowKeyError):
                # Type wasn't registered or doesn't exist, that's fine
                pass
        
        _extension_types_handled = True
        logger.debug("Extension type handling complete")
    except Exception as e:
        logger.warning(f"Could not handle extension types (this may be okay): {e}")
        _extension_types_handled = True


def safe_read_parquet(filepath):
    """
    Safely read a parquet file, handling extension type registration errors.
    
    Args:
        filepath: Path to the parquet file
        
    Returns:
        pandas.DataFrame or None if the file doesn't exist
    """
    if not os.path.isfile(filepath):
        return None
    
    try:
        handle_extension_type_registration()
        return pd.read_parquet(filepath)
    except (pa.ArrowKeyError, Exception) as e:
        if "already defined" in str(e):
            logger.warning(f"Extension type already registered, attempting fallback read for {filepath}")
            # Try reading with PyArrow directly to bypass pandas extension type registration
            try:
                import pyarrow.parquet as pq
                table = pq.read_table(filepath)
                return table.to_pandas()
            except Exception as e2:
                logger.error(f"Fallback parquet read failed: {e2}")
                raise e
        else:
            raise e


def safe_write_parquet(df, filepath):
    """
    Safely write a DataFrame to parquet, handling extension type registration errors.
    
    Args:
        df: pandas.DataFrame to write
        filepath: Path where to write the parquet file
    """
    try:
        handle_extension_type_registration()
        df.to_parquet(filepath)
    except (pa.ArrowKeyError, Exception) as e:
        if "already defined" in str(e):
            logger.warning(f"Extension type already registered, attempting fallback write for {filepath}")
            # Try writing with PyArrow directly
            try:
                import pyarrow.parquet as pq
                table = pa.Table.from_pandas(df)
                pq.write_table(table, filepath)
            except Exception as e2:
                logger.error(f"Fallback parquet write failed: {e2}")
                raise e
        else:
            raise e
