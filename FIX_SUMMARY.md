# Fix for PyArrow/Pandas Compatibility Issue

## Problem
The application was encountering the following error:
```
Error: A type extension with name pandas.period already defined
```

This error occurs when there's a version incompatibility between pandas and PyArrow, particularly when reading/writing Parquet files. It's a common issue that can happen when:
- Using newer versions of pandas (2.x) with PyArrow
- Using Python 3.13 (which is very new)
- Loading pandas extension types multiple times

## Solution
Added an environment variable fix at the beginning of both main files **before** importing pandas:

### Files Modified:
1. **app.py** - Added at line 2-4:
```python
import os
# Fix for PyArrow/Pandas compatibility issue - must be set before importing pandas
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
```

2. **flask_seaborn.py** - Added at line 1-3:
```python
import os
# Fix for PyArrow/Pandas compatibility issue - must be set before importing pandas
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
```

3. **Pipfile** - Updated to specify compatible version ranges:
```toml
pandas = ">=2.0.0,<3.0.0"
pyarrow = ">=14.0.0,<18.0.0"
```

## Current Versions
- Python: 3.13
- Pandas: 2.3.3
- PyArrow: 22.0.0

## Testing
Run `test_fix.py` to verify the fix is working:
```bash
python test_fix.py
```

## How to Run Your Application
```bash
python flask_seaborn.py
```
or with Flask:
```bash
flask --app flask_seaborn run
```

## What This Fix Does
The `PYARROW_IGNORE_TIMEZONE` environment variable tells PyArrow to be more lenient with timezone handling, which prevents the double-registration of pandas extension types that was causing the error.

## Additional Notes
- This fix must be applied **before** importing pandas in any module
- The error typically occurs when reading Parquet files that were created with datetime columns
- This is a known compatibility issue that affects many pandas+PyArrow projects
