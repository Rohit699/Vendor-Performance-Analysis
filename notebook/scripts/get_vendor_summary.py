import sqlite3
import pandas as pd
import logging
from Ingestion_data import ingest_data
import time

logging.basicConfig(
    filename ="Logs/get_vendor_summary.log",
    level =logging.DEBUG,
    format = "%{asctime)s - %{levelname}s - %{message}s",
    filemode ="a"
)
start = time.time()
def create_vendor_summary(conn):
    '''this function will merge the different tables to get the overall summary and adding new required columns in the resultant data'''
    
    vendor_sales_summary = pd.read_sql_query("""WITH FreightSummary AS (
    SELECT 
          VendorNumber , 
          sum(Freight) AS FreightCost
          FROM vendor_invoice
          Group by VendorNumber
    ),

    PurchaseSummary AS (
    SELECT 
          p.Brand,
          p.Description,
          p.VendorNumber,
    p.VendorName,
    p.PurchasePrice,
    pp.Volume,
    pp.Price AS Actual_Price,
    sum(p.Dollars) AS TotalPurchaseDollars,
    sum(p.Quantity) AS TotalPurchaseQuantity
    FROM purchases p JOIN
       purchase_prices pp
      ON p.Brand = pp.Brand
      where p.PurchasePrice > 0
      Group by
      p.VendorNumber , p.Brand, p.Description, p.PurchasePrice, pp.Price,pp.Volume
     ),

    SalesSummary AS (
    SELECT
    VendorNo,
    Brand,
    sum(SalesQuantity) AS TotalSalesQuantity,
    sum(SalesDollars) AS TotalSalesDollars,
    SalesPrice,
    sum(ExciseTax) AS TotalExciseTax
    FROM sales
    Group by VendorNo , Brand)

    SELECT 
   ps.VendorNumber ,
   ps.VendorName,
   ps.Brand,
   ps.Description,
   ps.Volume,
   ps.PurchasePrice,
   ps.Actual_Price,
   ps.TotalPurchaseQuantity,
   ps.TotalPurchaseDollars,
   fs.FreightCost,
   ss.SalesPrice,
   ss.TotalSalesQuantity,
   ss.TotalSalesDollars,
   ss.TotalExciseTax
   FROM PurchaseSummary ps JOIN
   SalesSummary ss
    ON ps.VendorNumber = ss.VendorNo AND ps.Brand = ss.Brand
      JOIN FreightSummary fs 
    ON fs.VendorNumber = ps.VendorNumber

    ORDER BY ps.TotalPurchaseDollars DESC """,conn)
   
    return vendor_sales_summary
    

    

def clean_data(df):
    ''' This function will clean the data'''
    start2= time.time()
    # changing the datatype to float
    df['Volume']= df['Volume'].astype('float64')

    # filling missing values from null to 0
    df.fillna(0, inplace=True)

    # Removing spaces from categorical values
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()

    # Creating new Columns for better Analysis
    vendor_sales_summary['GrossProfit'] = vendor_sales_summary['TotalSalesDollars'] - vendor_sales_summary['TotalPurchaseDollars']
    vendor_sales_summary['ProfitMargin'] = (vendor_sales_summary['GrossProfit']/vendor_sales_summary['TotalSalesDollars'])*100
    vendor_sales_summary['StockTurnover'] = vendor_sales_summary['TotalSalesQuantity']/vendor_sales_summary['TotalPurchaseQuantity']
    vendor_sales_summary['SalesToPurchaseRatio'] = vendor_sales_summary['TotalSalesDollars']/vendor_sales_summary['TotalPurchaseDollars']


    return df
    
end = time.time()
total_time = (end-start)/60

if __name__ == '__main__':
    # Creating Database Connection
    conn = sqlite3.connect('inventory.db')

    logging.info("Creating Vendor Summary Table.....")
    summary_df = create_vendor_summary(conn)
    logging.info(summary_df.head())

    logging.info("Cleaning Data.....")
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())

    logging.info("Ingesting Data......")
    ingest_data(clean_df , 'vendor_sales_summary' , conn)
    logging.info(f'Completed \n Time taken for merging and cleaning tables are{total_time} minutes')
    

    


