from sqlalchemy import create_engine
import pandas as pd
import logging
from load_data import ingest_db

# Make sure ingestion_db.py is in the same directory or in your PYTHONPATH
#from ingestion_db import ingest_db
# Make sure 'logs' folder exists
# if not os.path.exists('logs'):
#     os.makedirs('logs')

# Set up logging
logging.basicConfig(filename='logs/vendor_summary.log', level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filemode='a')


def create_vendor_summary(conn):
    """
    this function will merge the different tables to get the overall vendor summary and adding new column in the resultant  dta """
    vendor_sales_summary = pd.read_sql_query("""
WITH FreightSummary AS (
    SELECT
        "VendorNumber",
        SUM("Freight") AS FreightCost
    FROM vendor_invoice
    GROUP BY "VendorNumber"
),
PurchaseSummary AS (
    SELECT
        p."VendorNumber",
        p."VendorName",
        p."Brand",
        p."Description",
        p."PurchasePrice",
        pp."Price" AS ActualPrice,
        pp."Volume",
        SUM(p."Quantity") AS TotalPurchaseQuantity,
        SUM(p."Dollars") AS TotalPurchaseDollars
    FROM purchases p
    JOIN purchase_prices pp
        ON p."Brand" = pp."Brand"
    WHERE p."PurchasePrice" > 0
    GROUP BY 
        p."VendorNumber", p."VendorName", p."Brand", p."Description",
        p."PurchasePrice", pp."Price", pp."Volume"
),
SalesSummary AS (
    SELECT
        "VendorNo",
        "Brand",
        SUM("SalesQuantity") AS TotalSalesQuantity,
        SUM("SalesDollars") AS TotalSalesDollars,
        SUM("SalesPrice") AS TotalSalesPrice,
        SUM("ExciseTax") AS TotalExciseTax
    FROM sales
    GROUP BY "VendorNo", "Brand"
)
SELECT
    ps."VendorNumber",
    ps."VendorName",
    ps."Brand",
    ps."Description",
    ps."PurchasePrice",
    ps.ActualPrice,
    ps."Volume",
    ps.TotalPurchaseQuantity,
    ps.TotalPurchaseDollars,
    ss.TotalSalesQuantity,
    ss.TotalSalesDollars,
    ss.TotalSalesPrice,
    ss.TotalExciseTax,
    fs.FreightCost
FROM PurchaseSummary ps
LEFT JOIN SalesSummary ss
    ON ps."VendorNumber" = ss."VendorNo" AND ps."Brand" = ss."Brand"
LEFT JOIN FreightSummary fs
    ON ps."VendorNumber" = fs."VendorNumber"
ORDER BY ps.TotalPurchaseDollars DESC
""", conn)
    
    return vendor_sales_summary


def clean_data(df):
    """this function will clean the data """
   
    df['Volume'] = df['Volume'].astype('float64')
    df.fillna(0, inplace=True)
    #df.columns = df.columns.str.strip()
    df['VendorName'] = df['VendorName'].str.strip()
    df['gross_profit'] = df['totalsalesdollars'] - df['totalpurchasedollars']
    df['profitMargin']= (df['gross_profit'] / df['totalpurchasedollars']) * 100
    df['StockTurnover'] = df['totalsalesquantity'] / df['totalpurchasequantity']
    df['salespurchaseratio']= df['totalsalesdollars'] / df['totalpurchasedollars']

    return df

if __name__ == '__main__':
    # Create a database connection
    engine = create_engine('postgresql+psycopg2://postgres:root@localhost:5432/vendor_db')
    conn = engine.connect()

    logging.info("Creating vendor summary table...")
    summary_df=create_vendor_summary(conn)
    logging.info(summary_df.head() )

    logging.info("Cleaning data...")
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())


    logging.info("ingesting data.....")
    clean_df = clean_data(summary_df)
    ingest_db(clean_df,'vendor_sales_summary',conn)
    logging.info('completed')
