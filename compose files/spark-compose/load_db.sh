#!/bin/bash

python3 -m venv temp_env
echo "Virtual environment created."

source temp_env/bin/activate
echo "Virtual environment activated."

pip install pyhive
pip install thrift
pip install thrift_sasl
echo "packages installed."

python3 create_metastore.py

deactivate
rm -rf temp_env
echo "Virtual environment deleted."
