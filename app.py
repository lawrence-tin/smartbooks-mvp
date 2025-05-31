import streamlit as st
from PIL import Image
import easyocr
import io
import pandas as pd
import snowflake.connector
import numpy as np

@st.cache_resource(show_spinner=False)
def get_snowflake_connection():
    conn = snowflake.connector.connect(
        user=st.secrets["SNOWFLAKE_USER"],
        password=st.secrets["SNOWFLAKE_PASSWORD"],
        account=st.secrets["SNOWFLAKE_ACCOUNT"],
        warehouse=st.secrets["SNOWFLAKE_WAREHOUSE"],
        database=st.secrets["SNOWFLAKE_DATABASE"],
        schema=st.secrets["SNOWFLAKE_SCHEMA"],
        role=st.secrets.get("SNOWFLAKE_ROLE", None)
    )
    return conn

@st.cache_resource
def get_ocr_reader():
    return easyocr.Reader(['en'])

def extract_text_easyocr(image_bytes):
    image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    np_image = np.array(image)
    reader = get_ocr_reader()
    results = reader.readtext(np_image, detail=0)
    text = "\n".join(results)
    return text

def parse_invoice_text(text):
    lines = text.split('\n')
    data = {}
    for line in lines:
        if ':' in line:
            key, val = line.split(':', 1)
            data[key.strip().lower()] = val.strip()
    return data

def insert_raw_invoice_data(conn, filename, raw_text):
    cs = conn.cursor()
    try:
        sql = """
        INSERT INTO raw_invoices (filename, raw_text)
        VALUES (%s, %s)
        """
        cs.execute(sql, (filename, raw_text))
    finally:
        cs.close()

def insert_structured_invoice_data(conn, invoice_data):
    cs = conn.cursor()
    try:
        sql = """
        INSERT INTO structured_invoices (invoice_number, invoice_date, total_amount)
        VALUES (%s, %s, %s)
        """
        cs.execute(sql, (
            invoice_data.get('invoice number'),
            invoice_data.get('date'),
            invoice_data.get('total amount')
        ))
    finally:
        cs.close()

st.title("SmartBooks Invoice OCR and Dashboard")

uploaded_file = st.file_uploader("Upload Invoice Image", type=["png", "jpg", "jpeg", "tiff"])

if uploaded_file:
    image_bytes = uploaded_file.read()
    st.image(image_bytes, caption="Uploaded Invoice", use_column_width=True)
    
    st.info("Running OCR with EasyOCR...")
    raw_text = extract_text_easyocr(image_bytes)
    
    st.text_area("Raw OCR Text", raw_text, height=300)
    
    invoice_data = parse_invoice_text(raw_text)
    st.json(invoice_data)
    
    if st.button("Save Invoice Data to Snowflake"):
        conn = get_snowflake_connection()
        insert_raw_invoice_data(conn, uploaded_file.name, raw_text)
        insert_structured_invoice_data(conn, invoice_data)
        st.success("Invoice data saved successfully!")

if st.checkbox("Show Invoices Dashboard"):
    conn = get_snowflake_connection()
    df = pd.read_sql("SELECT * FROM structured_invoices ORDER BY invoice_date DESC LIMIT 20", conn)
    st.dataframe(df)
