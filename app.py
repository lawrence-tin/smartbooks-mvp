import streamlit as st
from PIL import Image
import easyocr
import io
import pandas as pd
import snowflake.connector
import numpy as np
import re
from dateutil import parser

# Cached Snowflake connection
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

# Cached OCR reader
@st.cache_resource
def get_ocr_reader():
    return easyocr.Reader(['en'])

# OCR extraction
def extract_text_easyocr(image_bytes):
    image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    np_image = np.array(image)
    reader = get_ocr_reader()
    results = reader.readtext(np_image, detail=0)
    text = "\n".join(results)
    return text

# Parse invoice text
def parse_invoice_text(text):
    data = {}

    # Common patterns
    invoice_number = re.search(r'Invoice\s*#?[\s:]*([\w-]+)', text, re.IGNORECASE)
    if invoice_number:
        data["invoice_number"] = invoice_number.group(1)

    invoice_date = re.search(r'Invoice Date[:\s]*([^\n]+)', text, re.IGNORECASE)
    if invoice_date:
        try:
            data["invoice_date"] = str(parser.parse(invoice_date.group(1), fuzzy=True).date())
        except:
            pass

    due_date = re.search(r'Due Date[:\s]*([^\n]+)', text, re.IGNORECASE)
    if due_date:
        try:
            data["due_date"] = str(parser.parse(due_date.group(1), fuzzy=True).date())
        except:
            pass

    client = re.search(r'Invoiced To\s*\n(.+)', text, re.IGNORECASE)
    if client:
        data["client_name"] = client.group(1).strip()

    address_lines = re.findall(r'Invoiced To\s*\n.*\n(.*)\n(.*)\n?(.*)?', text)
    if address_lines:
        addr = address_lines[0]
        data["client_address_line1"] = addr[0].strip()
        data["client_address_line2"] = addr[1].strip()
        data["client_address_line3"] = addr[2].strip() if len(addr) > 2 and addr[2] else ""

    vat = re.search(r'VAT Number[:\s]*([A-Z0-9]+)', text, re.IGNORECASE)
    if vat:
        data["vendor_vat_number"] = vat.group(1)

    reg = re.search(r'Reg(?:istration)? Number[:\s]*([A-Z0-9]+)', text, re.IGNORECASE)
    if reg:
        data["vendor_reg_number"] = reg.group(1)

    bank = re.search(r'Bank[:\s]*(.+)', text, re.IGNORECASE)
    if bank:
        data["vendor_bank"] = bank.group(1).strip()

    account = re.search(r'Account Number[:\s]*(\d+)', text, re.IGNORECASE)
    if account:
        data["vendor_bank_account"] = account.group(1)

    subtotal = re.search(r'Subtotal[:\s]*R?([\d,.]+)', text, re.IGNORECASE)
    if subtotal:
        data["subtotal"] = subtotal.group(1).replace(',', '')

    tax_amount = re.search(r'Tax\s*(?:Amount)?[:\s]*R?([\d,.]+)', text, re.IGNORECASE)
    if tax_amount:
        data["tax_amount"] = tax_amount.group(1).replace(',', '')

    tax_percent = re.search(r'(\d{1,2}\.?\d{0,2})%\s+SA', text)
    if tax_percent:
        data["tax_percent"] = tax_percent.group(1)

    total = re.search(r'Total[:\s]*R?([\d,.]+)', text, re.IGNORECASE)
    if total:
        data["total_amount"] = total.group(1).replace(',', '')

    balance = re.search(r'Balance Due[:\s]*R?([\d,.]+)', text, re.IGNORECASE)
    if balance:
        data["balance"] = balance.group(1).replace(',', '')

    status = "UNPAID" if "UNPAID" in text.upper() else "PAID" if "PAID" in text.upper() else None
    if status:
        data["status"] = status

    # Additional defaults
    data["vendor_name"] = "Not Detected"
    data["vendor_address"] = "Not Detected"
    data["description"] = "General Invoice"
    data["currency"] = "ZAR"

    return data

# Insert raw OCR text
def insert_raw_invoice_data(conn, filename, raw_text):
    cs = conn.cursor()
    try:
        cs.execute("INSERT INTO raw_invoices (filename, raw_text) VALUES (%s, %s)", (filename, raw_text))
    finally:
        cs.close()

# Insert structured invoice data
def insert_structured_invoice_data(conn, invoice_data):
    cs = conn.cursor()
    try:
        sql = """
        INSERT INTO structured_invoices (
            invoice_number, invoice_date, due_date,
            client_name, client_address_line1, client_address_line2, client_address_line3,
            vendor_name, vendor_address, vendor_vat_number, vendor_reg_number,
            vendor_bank, vendor_bank_account,
            description, subtotal, tax_percent, tax_amount,
            total_amount, balance,
            status, currency
        ) VALUES (
            %(invoice_number)s, %(invoice_date)s, %(due_date)s,
            %(client_name)s, %(client_address_line1)s, %(client_address_line2)s, %(client_address_line3)s,
            %(vendor_name)s, %(vendor_address)s, %(vendor_vat_number)s, %(vendor_reg_number)s,
            %(vendor_bank)s, %(vendor_bank_account)s,
            %(description)s, %(subtotal)s, %(tax_percent)s, %(tax_amount)s,
            %(total_amount)s, %(balance)s,
            %(status)s, %(currency)s
        )
        """
        cs.execute(sql, invoice_data)
    finally:
        cs.close()

# Streamlit UI
st.title("📄 SmartBooks Invoice OCR & Snowflake")

uploaded_file = st.file_uploader("Upload an Invoice (JPG/PNG)", type=["jpg", "jpeg", "png"])

if uploaded_file:
    image_bytes = uploaded_file.read()
    st.image(image_bytes, caption="Invoice Preview", use_column_width=True)

    with st.spinner("Running OCR..."):
        raw_text = extract_text_easyocr(image_bytes)
    st.text_area("🧾 Raw OCR Text", raw_text, height=300)

    invoice_data = parse_invoice_text(raw_text)
    st.subheader("📊 Extracted Invoice Data")
    st.json(invoice_data)

    if st.button("✅ Save to Snowflake"):
        conn = get_snowflake_connection()
        insert_raw_invoice_data(conn, uploaded_file.name, raw_text)
        insert_structured_invoice_data(conn, invoice_data)
        st.success("Invoice data saved to Snowflake!")

# Dashboard view
if st.checkbox("📈 Show Latest Invoices"):
    conn = get_snowflake_connection()
    df = pd.read_sql("SELECT * FROM structured_invoices ORDER BY inserted_at DESC LIMIT 20", conn)
    st.dataframe(df)
