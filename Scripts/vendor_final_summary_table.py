import pandas as pd
import os
from sqlalchemy import create_engine
import logging
from ingestion_db import ingest_db
import time

'''Setup logging'''
logging.basicConfig(
    filename="logs/vendor_final_summary_table.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)


engine = create_engine("mysql+pymysql://root:Sanjana%401611@localhost:3306/inventorydb")


def create_final_summary(con=engine):
    start = time.time()
    '''this function will merge different tables to get the overall vendor summary & adding new columns in the data'''
    final_table = pd.read_sql_query("""SELECT
        pp.VendorNumber,
        pp.VendorName,
        pp.Brand,
        pp.Description,
        pp.Price AS ActualPrice,
        pp.PurchasePrice,
        pp.Volume ,
        SUM(s.SalesQuantity) AS TotalSalesQuantity,
        SUM(s.SalesDollars) AS TotalSalesDollars,
        SUM(s.SalesPrice) AS TotalSalesPrice,
        SUM(s.ExciseTax) AS TotalExciseTax,
        SUM(vi.Quantity) AS TotalPurchaseQuantity,
        SUM(vi.Dollars) AS TotalPurchaseDollars,
        SUM(vi.Freight) AS TotalFreightCost
        FROM purchase_prices1 pp
        JOIN sales1 s
        ON pp.VendorNumber = s.VendorNO
        AND pp.Brand = s.Brand
        JOIN vendor_invoice1 vi
        ON pp.VendorNumber = vi.VendorNumber
        GROUP BY pp.VendorNumber,
        pp.VendorName,
        pp.Brand,
        pp.Description,
        pp.Price,
        pp.PurchasePrice,
        pp.Volume 
        """, con=engine)
    end = time.time()
    print(f"Query executed in {end - start:.2f} seconds.")

    return final_table


def clean_data(df):
    '''this function will clean the data'''
    # changing datatype to float(I did not use)
    final_table['Volume'] = final_table['Volume'].astype('float')
  

    # filling missing value with 0(I did not use)
    final_table.fillna(0,inplace = True)

    # removing spaces from categorial columns
    final_table['VendorName'] = final_table['VendorName'].str.strip()
    final_table['Description'] = final_table['Description'].str.strip()
    
    # to view after spaces
    final_table['VendorName'].unique()
    final_table['Description'].unique()

    # creating new columns for better analysis
    final_table['GrossProfit'] = final_table['TotalSalesDollars'] - final_table['TotalPurchaseDollars']
    final_table['ProfitMargin'] = (final_table['GrossProfit'] / final_table['TotalSalesDollars'])*100
    final_table['StockTurnover'] = final_table['TotalSalesQuantity'] / final_table['TotalPurchaseQuantity']
    final_table['SalesPurchaseRatio'] = final_table['TotalSalesDollars'] / final_table['TotalPurchaseDollars']

    return final_table


if __name__ == '__main__':
    # creating database connection
    con = engine.connect()

    logging.info('Creating Vendor Summary Table......')
    summary_final_table = create_final_summary(con)
    logging.info(summary_final_table.head())

    logging.info('Cleaning Data......')
    clean_final_table = clean_data(summary_final_table)
    logging.info(clean_final_table.head())

    logging.info('Ingesting data......')
    ingest_db(clean_final_table,'vendor_final_summary',con)
    logging.info('Completed')


    # created new table(from here she did not mention to add)
    from sqlalchemy import text
    con.execute(text("""CREATE TABLE final_table (
    VendorNumber INT,            
    VendorName VARCHAR(100),               
    Brand INT,
    Description VARCHAR(100),              
    ActualPrice DECIMAL(10,2),             
    PurchasePrice DECIMAL(10,2),           
    Volume INT ,                  
    TotalSalesQuantity INT,      
    TotalSalesDollars DECIMAL(15,2),       
    TotalSalesPrice DECIMAL(15,2),         
    TotalExciseTax DECIMAL(15,2),          
    TotalPurchaseQuantity INT,  
    TotalPurchaseDollars DECIMAL(15,2),    
    TotalFreightCost DECIMAL(15,2),   
    GrossProfit DECIMAL(15,2),
    ProfitMargin DECIMAL(15,2),
    StockTurnover DECIMAL(15,2),
    SalesPurchaseRatio DECIMAL(15,2),
    PRIMARY KEY (VendorNumber, Brand)
    );
    """))

    #reading data to newly created table
    pd.read_sql_query("select *from final_table",con=engine )

    #to read how many rows are present
    final_table.to_sql('final_table', con=engine, if_exists='replace', index=False)



    


    
 
















    