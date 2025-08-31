import streamlit as st

from pymongo import MongoClient

MONGO_URL = st.secrets["MONGO_URL"]
MONGO_DB = st.secrets["MONGO_DB"]

def create_connection():
    client = MongoClient(MONGO_URL)
    db = client[MONGO_DB]
    return db
