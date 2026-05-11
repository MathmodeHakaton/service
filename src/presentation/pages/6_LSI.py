import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import streamlit as st

st.set_page_config(page_title="LSI · Агрегация", page_icon="📊", layout="wide")
st.title("LSI — Liquidity Stress Index")
st.info("Страница в разработке.")
