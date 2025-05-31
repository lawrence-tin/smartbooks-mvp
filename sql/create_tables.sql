CREATE OR REPLACE TABLE raw_invoices (
    id INTEGER AUTOINCREMENT,
    filename STRING,
    raw_text STRING,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE TABLE structured_invoices (
    id INTEGER AUTOINCREMENT,
    invoice_number STRING,
    invoice_date DATE,
    total_amount FLOAT,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
