#! /bin/sh

# create virtual environment
python -m venv venv

# activate virtual environment
source venv/bin/activate

# install dependencies
pip install -r requirements.txt