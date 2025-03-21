#!/bin/bash

# Install test dependencies
pip install -r requirements-test.txt

# Run the tests with pytest
pytest test_ingestion.py -v --asyncio-mode=auto 