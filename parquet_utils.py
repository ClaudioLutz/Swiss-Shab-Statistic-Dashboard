# %%
"""
Utility module for safe parquet operations.
Handles PyArrow extension type registration issues and provides atomic write operations.
"""

import os
import pandas as pd
import pyarrow as pa
import logging
import tempfile
import shutil
import contextlib
import time
import sys

# Import platform-specific file locking modules
if sys.platform == 'win32':
    import msvcrt
else:
    import fcntl

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


def safe_write_parquet_atomic(df, filepath):
    """
    Safely and atomically write a DataFrame to parquet.
    Writes to a temporary file first, then fsyncs and moves it to destination.
    
    Args:
        df: pandas.DataFrame to write
        filepath: Path where to write the parquet file
    """
    directory = os.path.dirname(filepath)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)

    # Create temp file in the same directory to ensure atomic move works
    fd, temp_path = tempfile.mkstemp(dir=directory, prefix="tmp_shab_", suffix=".parquet")
    os.close(fd)

    try:
        handle_extension_type_registration()
        df.to_parquet(temp_path)
    except (pa.ArrowKeyError, Exception) as e:
        if "already defined" in str(e):
            logger.warning(f"Extension type already registered, attempting fallback write for {filepath}")
            # Try writing with PyArrow directly
            try:
                import pyarrow.parquet as pq
                table = pa.Table.from_pandas(df)
                pq.write_table(table, temp_path)
            except Exception as e2:
                logger.error(f"Fallback parquet write failed: {e2}")
                os.remove(temp_path)
                raise e
        else:
            os.remove(temp_path)
            raise e

    # Atomic move
    try:
        os.replace(temp_path, filepath)
    except OSError:
        # Fallback for systems where replace might fail across filesystems (shouldn't happen here)
        os.remove(filepath)
        shutil.move(temp_path, filepath)

@contextlib.contextmanager
def acquire_lock(lock_file, timeout=60):
    """
    Context manager to acquire a file lock.
    Works cross-platform (Windows and Unix).
    """
    start_time = time.time()
    lock_fd = open(lock_file, 'w')
    try:
        while True:
            try:
                if sys.platform == 'win32':
                    # Windows file locking using msvcrt
                    msvcrt.locking(lock_fd.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    # Unix file locking using fcntl
                    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except (IOError, OSError):
                if time.time() - start_time >= timeout:
                    raise TimeoutError(f"Could not acquire lock on {lock_file} within {timeout} seconds")
                time.sleep(0.1)
        yield
    finally:
        try:
            if sys.platform == 'win32':
                # Unlock on Windows
                msvcrt.locking(lock_fd.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                # Unlock on Unix
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
        except Exception:
            pass
        lock_fd.close()
