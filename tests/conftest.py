import sys
import os

# Ajoute la racine du projet au PYTHONPATH
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

import pytest
import pandas as pd
from etl.extract import extract_matches
from etl.transform import transform_matches


@pytest.fixture(scope="session")
def df_raw():
    return extract_matches("data_clean/matches_1930_2022_clean.csv")


@pytest.fixture(scope="session")
def tables(df_raw):
    return transform_matches(df_raw)
