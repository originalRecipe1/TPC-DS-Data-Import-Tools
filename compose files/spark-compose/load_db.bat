@echo off

REM Create virtual environment
python -m venv temp_env
echo Virtual environment created.

REM Activate virtual environment
call temp_env\Scripts\activate.bat
echo Virtual environment activated.

REM Install dependencies
pip install pyhive
pip install thrift
pip install thrift_sasl
echo Packages installed.

REM Run your Python script
python create_metastore.py

REM Deactivate virtual environment
call temp_env\Scripts\deactivate.bat

REM Remove the virtual environment folder
rmdir /s /q temp_env
echo Virtual environment deleted.
