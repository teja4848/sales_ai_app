### Utility Functions
import pandas as pd
import sqlite3
import datetime
from sqlite3 import Error

def create_connection(db_file, delete_db=False):
    import os
    if delete_db and os.path.exists(db_file):
        os.remove(db_file)

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql, drop_table_name=None):
    
    if drop_table_name: # You can optionally pass drop_table_name to drop the table. 
        try:
            #temporary disable foreign keys
            c = conn.cursor()
            c.execute("PRAGMA foreign_keys = 0")
            c.execute("""DROP TABLE IF EXISTS %s""" % (drop_table_name))
            c.execute("PRAGMA foreign_keys = 1")
        except Error as e:
            print(e)
    
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
        
def execute_sql_statement(sql_statement, conn):
    cur = conn.cursor()
    cur.execute(sql_statement)

    rows = cur.fetchall()

    return rows

def step1_create_region_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    conn=create_connection(normalized_database_filename)
    create_table(conn,"CREATE TABLE Region( RegionID INTEGER PRIMARY KEY,Region TEXT)", "Region")
    with open(data_filename) as f: next(f); conn.executemany("INSERT INTO Region (Region) VALUES(?)", sorted({(l.strip().split('\t')[4],) for l in f if len(l.split('\t'))>4}))
    conn.commit(); conn.close()

def step2_create_region_to_regionid_dictionary(normalized_database_filename):
    conn=create_connection(normalized_database_filename)
    data={r[0]:r[1] for r in execute_sql_statement("select region,regionid from region",conn)}
    conn.close(); return data

def step3_create_country_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    rmap=step2_create_region_to_regionid_dictionary(normalized_database_filename)
    conn=create_connection(normalized_database_filename)
    create_table(conn,"CREATE TABLE Country( CountryID INTEGER PRIMARY KEY, Country TEXT, RegionID INTEGER, FOREIGN KEY(RegionID) REFERENCES Region(RegionID))","Country")
    with open(data_filename) as f: next(f); conn.executemany("INSERT INTO Country (Country,RegionID) VALUES (?,?)",sorted({(l.strip().split('\t')[3],rmap[l.strip().split('\t')[4]]) for l in f if len(l.split('\t'))>4 and l.strip().split('\t')[4] in rmap},key=lambda x: x[0]))
    conn.commit(); conn.close()

def step4_create_country_to_countryid_dictionary(normalized_database_filename):
    conn=create_connection(normalized_database_filename)
    data= {r[0]:r[1] for r in execute_sql_statement("select country,countryid from country",conn)}
    conn.close(); return data
        

def step5_create_customer_table(data_filename, normalized_database_filename):
    cmap=step4_create_country_to_countryid_dictionary(normalized_database_filename)
    conn=create_connection(normalized_database_filename)
    create_table(conn,"CREATE TABLE Customer(CustomerID INTEGER PRIMARY KEY, FirstName TEXT,LastName TEXT, Address TEXT, City TEXT, CountryID INTEGER, FOREIGN KEY(CountryID) REFERENCES Country(CountryID))", "Customer")
    with open(data_filename) as f:
      next(f);data=[]
      for l in f:
          p=l.strip().split('\t')
          if len(p)>4 and p[3] in cmap:
              nm=p[0].split();data.append((nm[0],' '.join(nm[1:]),p[1],p[2],cmap[p[3]]))
      conn.executemany("INSERT INTO Customer (FirstName, LastName, Address, City, CountryID) VALUES (?, ?, ?, ?, ?)", sorted(list(set(data)), key=lambda x: x[0] + " " + x[1]))
    conn.commit(); conn.close()

def step6_create_customer_to_customerid_dictionary(normalized_database_filename):
    conn=create_connection(normalized_database_filename)
    data= {f"{r[0]} {r[1]}":r[2] for r in execute_sql_statement("select firstname, lastname, customerid from customer",conn)}
    conn.close(); return data


def step7_create_productcategory_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    conn=create_connection(normalized_database_filename)
    create_table(conn,"CREATE TABLE ProductCategory(ProductCategoryID INTEGER PRIMARY KEY, ProductCategory TEXT, ProductCategoryDescription TEXT)","ProductCategory")
    with open(data_filename) as f: next(f); conn.executemany("INSERT INTO ProductCategory (ProductCategory, ProductCategoryDescription) VALUES (?, ?)", sorted({(cat, desc) for l in f if len(l.split('\t'))>7 for cat, desc in zip(l.strip().split('\t')[6].split(';'), l.strip().split('\t')[7].split(';'))}))
    conn.commit(); conn.close()

def step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename):
    conn=create_connection(normalized_database_filename)
    data= {r[0]:r[1] for r in execute_sql_statement("select productcategory,productcategoryid from productcategory",conn)}
    conn.close(); return data
    
    
def step9_create_product_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    catmap=step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename)
    conn=create_connection(normalized_database_filename)
    create_table(conn,"CREATE TABLE Product(ProductID INTEGER PRIMARY KEY, ProductName TEXT, ProductUnitPrice REAL, ProductCategoryID INTEGER, FOREIGN KEY(ProductCategoryID) REFERENCES ProductCategory(ProductCategoryID))","Product")
    with open(data_filename) as f: next(f); conn.executemany("INSERT INTO product(ProductName,ProductUnitPrice, ProductCategoryID) VALUES(?,?,?)",sorted({(n,float(p),catmap[ca]) for l in f if len(l.split('\t'))>8 for n,ca,p in zip(l.strip().split('\t')[5].split(';'), l.strip().split('\t')[6].split(';'), l.strip().split('\t')[8].split(';')) if ca in catmap},key=lambda x:x[0]))
    conn.commit(); conn.close()

def step10_create_product_to_productid_dictionary(normalized_database_filename):
    conn=create_connection(normalized_database_filename)
    data={r[0]:r[1] for r in execute_sql_statement("select productname,productid from product",conn)}
    conn.close(); return data
  
     
def step11_create_orderdetail_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    custmap,prodmap=step6_create_customer_to_customerid_dictionary(normalized_database_filename), step10_create_product_to_productid_dictionary(normalized_database_filename)
    conn=create_connection(normalized_database_filename)
    create_table(conn,"CREATE TABLE OrderDetail(OrderID INTEGER PRIMARY KEY,CustomerID INTEGER, ProductID INTEGER, OrderDate TEXT, QuantityOrdered INETGER, FOREIGN KEY(CustomerID) REFERENCES Customer(CustomerID), FOREIGN KEY(ProductID) REFERENCES Product(ProductID))","OrderDetail")
    with open(data_filename) as f:
      next(f);orders=[]
      for l in f:
          p=l.strip().split('\t')
          if len(p)>10:
            cname=' '.join(p[0].split())
            if cname in custmap:
              cid = custmap[cname]
              pnames = [x.strip() for x in p[5].split(';')]
              qtys = p[9].split(';')
              dates = p[10].split(';')  
              for pn, q, dt in zip(pnames, qtys, dates):
                  if pn in prodmap:
                      orders.append((
                        cid, 
                        prodmap[pn], 
                        datetime.datetime.strptime(dt.strip(), '%Y%m%d').strftime('%Y-%m-%d'), 
                        int(q)
                      ))
      conn.executemany("INSERT INTO OrderDetail (CustomerID, ProductID, OrderDate, QuantityOrdered) VALUES(?,?,?,?)",orders)
      conn.commit(); conn.close()


def ex1(conn, CustomerName):   
    # Simply, you are fetching all the rows for a given CustomerName. 
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # ProductName
    # OrderDate
    # ProductUnitPrice
    # QuantityOrdered
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    
    sql_statement = f"select c.firstname || ' ' || c.lastname as Name, p.productname as ProductName, o.orderdate as OrderDate, p.productunitprice as ProductUnitPrice,o.quantityordered as QuantityOrdered,round(p.productunitprice * o.quantityordered,2) as Total from orderdetail o join customer c on o.customerid=c.customerid join product p on o.productid=p.productid where (c.firstname || ' ' || c.lastname)='{CustomerName}'"
    '''
    sql_statement=f"""
        SELECT 
            C.FirstName || ' ' || C.LastName as Name, 
            P.ProductName, 
            O.OrderDate, 
            P.ProductUnitPrice, 
            O.QuantityOrdered, 
            ROUND(P.ProductUnitPrice * O.QuantityOrdered, 2) as Total 
        FROM OrderDetail O 
        JOIN Customer C ON O.CustomerID = C.CustomerID 
        JOIN Product P ON O.ProductID = P.ProductID 
        WHERE (C.FirstName || ' ' || C.LastName) = '{CustomerName}'
    """
    '''
    return sql_statement

def ex2(conn, CustomerName):   
    # Simply, you are summing the total for a given CustomerName. 
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    
    sql_statement = f"select c.firstname || ' ' || c.lastname as Name, round(sum(p.productunitprice * o.quantityordered),2) as Total from orderdetail o join customer c on o.customerid=c.customerid join product p on o.productid=p.productid where (c.firstname || ' ' || c.lastname)='{CustomerName}' group by c.customerid"
    '''
    sql_statement= f"""
        SELECT 
            C.FirstName || ' ' || C.LastName as Name, 
            ROUND(SUM(ROUND(P.ProductUnitPrice * O.QuantityOrdered, 2)), 2) as Total 
        FROM OrderDetail O 
        JOIN Customer C ON O.CustomerID = C.CustomerID 
        JOIN Product P ON O.ProductID = P.ProductID 
        WHERE (C.FirstName || ' ' || C.LastName) = '{CustomerName}' 
        GROUP BY C.CustomerID
    """
    '''
    return sql_statement

def ex3(conn):   
    # Simply, find the total for all the customers
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending 
    
    sql_statement = "select c.firstname || ' ' || c.lastname as Name, round(sum(p.productunitprice * o.quantityordered),2) as Total from orderdetail o join customer c on o.customerid = c.customerid join product p on o.productid = p.productid group by c.customerid order by total desc"
    '''
    sql_statement="""
        SELECT 
            C.FirstName || ' ' || C.LastName as Name, 
            ROUND(SUM(P.ProductUnitPrice * O.QuantityOrdered), 2) as Total 
        FROM OrderDetail O 
        JOIN Customer C ON O.CustomerID = C.CustomerID 
        JOIN Product P ON O.ProductID = P.ProductID 
        GROUP BY C.CustomerID 
        ORDER BY Total DESC
    """
    '''
    return sql_statement

def ex4(conn):   
    # Simply, find the total for all the region
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, Country, and 
    # Region tables.
    # Pull out the following columns. 
    # Region
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending 
    
    sql_statement = "select r.region,round(sum(p.productunitprice * o.quantityordered),2) as Total from orderdetail o join customer c on o.customerid=c.customerid join country co on c.countryid=co.countryid join region r on co.regionid=r.regionid join product p on o.productid=p.productid group by r.region order by total desc"
    '''
    sql_statement="""
        SELECT 
            R.Region, 
            ROUND(SUM(P.ProductUnitPrice * O.QuantityOrdered), 2) as Total 
        FROM OrderDetail O 
        JOIN Customer C ON O.CustomerID = C.CustomerID 
        JOIN Country Co ON C.CountryID = Co.CountryID 
        JOIN Region R ON Co.RegionID = R.RegionID 
        JOIN Product P ON O.ProductID = P.ProductID 
        GROUP BY R.Region 
        ORDER BY Total DESC
    """
    '''
    return sql_statement

def ex5(conn):   
    # Simply, find the total for all the countries
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, and Country table.
    # Pull out the following columns. 
    # Country
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round
    # ORDER BY Total Descending 

    sql_statement = "select co.country as Country,round(sum(p.productunitprice * o.quantityordered)) as Total from orderdetail o join customer c on o.customerid = c.customerid join country co on c.countryid = co.countryid join product p on o.productid=p.productid group by co.country order by total desc"
    '''
    sql_statement="""
        SELECT 
            Co.Country, 
            ROUND(SUM(P.ProductUnitPrice * O.QuantityOrdered)) as Total 
        FROM OrderDetail O 
        JOIN Customer C ON O.CustomerID = C.CustomerID 
        JOIN Country Co ON C.CountryID = Co.CountryID 
        JOIN Product P ON O.ProductID = P.ProductID 
        GROUP BY Co.Country 
        ORDER BY Total DESC
      """
      '''
    return sql_statement

def ex6(conn):   
    # Rank the countries within a region based on order total
    # Output Columns: Region, Country, CountryTotal, TotalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region

    sql_statement = "select r.region as Region, co.country as Country, round(sum(p.productunitprice * o.quantityordered)) as CountryTotal, rank() over (partition by r.region order by sum(p.productunitprice * o.quantityordered) desc) as TotalRank from orderdetail o join customer c on o.customerid = c.customerid join country co on c.countryid = co.countryid join region r on co.regionid=r.regionid join product p on o.productid=p.productid group by r.region, co.country order by r.region asc"
    '''
    SELECT 
        R.Region, 
        Co.Country, 
        ROUND(SUM(P.ProductUnitPrice * O.QuantityOrdered)) as CountryTotal, 
        RANK() OVER (PARTITION BY R.Region ORDER BY SUM(P.ProductUnitPrice * O.QuantityOrdered) DESC) as TotalRank 
    FROM OrderDetail O 
    JOIN Customer C ON O.CustomerID = C.CustomerID 
    JOIN Country Co ON C.CountryID = Co.CountryID 
    JOIN Region R ON Co.RegionID = R.RegionID 
    JOIN Product P ON O.ProductID = P.ProductID 
    GROUP BY R.Region, Co.Country 
    ORDER BY R.Region ASC
    '''
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def ex7(conn):   
    # Rank the countries within a region based on order total, BUT only select the TOP country, meaning rank = 1!
    # Output Columns: Region, Country, Total, TotalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region
    # HINT: Use "WITH"

    sql_statement = "with RankedCountries as (select r.region, co.country,round(sum(p.productunitprice * o.quantityordered)) as CountryTotal, rank () over (partition by r.region order by sum (p.productunitprice * o.quantityordered) desc) as CountryRegionalRank from orderdetail o join customer c on o.customerid = c.customerid join country co on c.countryid = co.countryid join region r on co.regionid=r.regionid join product p on o.productid=p.productid group by r.region, co.country) select region as Region, country as Country,CountryTotal, CountryRegionalRank from RankedCountries where CountryRegionalRank = 1 order by region asc, country asc" 
    '''
    WITH RankedCountries AS (
        SELECT 
            R.Region, 
            Co.Country, 
            ROUND(SUM(P.ProductUnitPrice * O.QuantityOrdered)) as CountryTotal, 
            RANK() OVER (PARTITION BY R.Region ORDER BY SUM(P.ProductUnitPrice * O.QuantityOrdered) DESC) as TotalRank 
        FROM OrderDetail O 
        JOIN Customer C ON O.CustomerID = C.CustomerID 
        JOIN Country Co ON C.CountryID = Co.CountryID 
        JOIN Region R ON Co.RegionID = R.RegionID 
        JOIN Product P ON O.ProductID = P.ProductID 
        GROUP BY R.Region, Co.Country
    ) 
    SELECT Region, Country, Total, TotalRank 
    FROM RankedCountries 
    WHERE TotalRank = 1 
    ORDER BY Region ASC
    '''
    return sql_statement

def ex8(conn):   
    # Sum customer sales by Quarter and year
    # Output Columns: Quarter,Year,CustomerID,Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!

    sql_statement = "select case when strftime('%m',orderdate) between '01' and '03' then 'Q1' when strftime('%m',orderdate) between '04' and '06' then 'Q2' when strftime('%m',orderdate) between '07' and '09' then 'Q3' else 'Q4' end as Quarter, cast(strftime('%Y',orderdate) as integer) as Year,customerid as CustomerID, round(sum(p.productunitprice * o.quantityordered)) as Total from orderdetail o join product p on o.productid = p.productid group by Quarter, Year, CustomerID order by Year,Quarter, CustomerID"
    '''
    SELECT 
        CASE 
            WHEN strftime('%m', OrderDate) BETWEEN '01' AND '03' THEN 'Q1' 
            WHEN strftime('%m', OrderDate) BETWEEN '04' AND '06' THEN 'Q2' 
            WHEN strftime('%m', OrderDate) BETWEEN '07' AND '09' THEN 'Q3' 
            ELSE 'Q4' 
        END as Quarter, 
        CAST(strftime('%Y', OrderDate) AS INTEGER) as Year, 
        CustomerID, 
        ROUND(SUM(P.ProductUnitPrice * O.QuantityOrdered)) as Total 
    FROM OrderDetail O 
    JOIN Product P ON O.ProductID = P.ProductID 
    ORDER BY Quarter, Year, CustomerID
    '''
    return sql_statement

def ex9(conn):    
    # Rank the customer sales by Quarter and year, but only select the top 5 customers!
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!
    # HINT: You can have multiple CTE tables;
    # WITH table1 AS (), table2 AS ()

    sql_statement = "with QuarterlySales as (select case when strftime('%m',orderdate) between '01' and '03' then 'Q1' when strftime('%m',orderdate) between '04' and '06' then 'Q2' when strftime('%m',orderdate) between '07' and '09' then 'Q3' else 'Q4' end as Quarter, cast(strftime('%Y',orderdate) as integer) as Year,customerid as CustomerID, round(sum(p.productunitprice * o.quantityordered)) as Total from orderdetail o join product p on o.productid = p.productid group by Quarter, Year, CustomerID), RankedSales as (select Quarter, Year, CustomerID,Total, rank() over (partition by Year, Quarter order by Total desc) as CustomerRank from QuarterlySales) select Quarter,Year, CustomerID,Total,CustomerRank from RankedSales where CustomerRank<=5 order by Year, Quarter, Total desc"
    '''
     WITH QuarterlySales AS (
            SELECT 
                CASE 
                    WHEN strftime('%m', OrderDate) BETWEEN '01' AND '03' THEN 'Q1' 
                    WHEN strftime('%m', OrderDate) BETWEEN '04' AND '06' THEN 'Q2' 
                    WHEN strftime('%m', OrderDate) BETWEEN '07' AND '09' THEN 'Q3' 
                    ELSE 'Q4' 
                END as Quarter, 
                CAST(strftime('%Y', OrderDate) AS INTEGER) as Year, 
                CustomerID, 
                ROUND(SUM(P.ProductUnitPrice * O.QuantityOrdered)) as Total 
            FROM OrderDetail O 
            JOIN Product P ON O.ProductID = P.ProductID 
            GROUP BY Quarter, Year, CustomerID
        ), 
        RankedSales AS (
            SELECT 
                Quarter, 
                Year, 
                CustomerID, 
                Total, 
                RANK() OVER (PARTITION BY Year, Quarter ORDER BY Total DESC) as SalesRank 
            FROM QuarterlySales
        ) 
        SELECT Quarter, Year, CustomerID, Total 
        FROM RankedSales 
        WHERE SalesRank <= 5
    '''
    return sql_statement

def ex10(conn):    
    # Rank the monthy sales
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total

    sql_statement = "with MonthlyTotals as (select strftime('%m', orderdate) as MonthNum, sum(round(p.productunitprice * o.quantityordered)) as Total from orderdetail o join product p on o.productid = p.productid group by MonthNum) select case MonthNum when '01' then 'January' when '02' then 'February' when '03' then 'March' when '04' then 'April' when '05' then 'May' when '06' then 'June' when '07' then 'July' when '08' then 'August' when '09' then 'September' when '10' then 'October' when '11' then 'November' when '12' then 'December' end as Month, Total, rank() over (order by Total desc) as TotalRank from MonthlyTotals order by TotalRank"
    '''
      WITH MonthlyTotals AS (
        SELECT
            strftime('%m', O.OrderDate) AS MonthNum,
            SUM(ROUND(P.ProductUnitPrice * O.QuantityOrdered)) AS Total
        FROM OrderDetail O
        JOIN Product P ON O.ProductID = P.ProductID
        GROUP BY MonthNum
    ),
    MonthNames AS (
        SELECT
            CASE MonthNum
                WHEN '01' THEN 'January'
                WHEN '02' THEN 'February'
                WHEN '03' THEN 'March'
                WHEN '04' THEN 'April'
                WHEN '05' THEN 'May'
                WHEN '06' THEN 'June'
                WHEN '07' THEN 'July'
                WHEN '08' THEN 'August'
                WHEN '09' THEN 'September'
                WHEN '10' THEN 'October'
                WHEN '11' THEN 'November'
                WHEN '12' THEN 'December'
            END AS Month,
            Total
        FROM MonthlyTotals
    )
    SELECT
        Month,
        Total * 1.0 AS Total,  -- forces float with .0
        RANK() OVER (ORDER BY Total DESC) AS TotalRank
    FROM MonthNames
    ORDER BY Total DESC;
    '''
    return sql_statement

def ex11(conn):   
    # Find the MaxDaysWithoutOrder for each customer 
    # Output Columns: 
    # CustomerID,
    # FirstName,
    # LastName,
    # Country,
    # OrderDate, 
    # PreviousOrderDate,
    # MaxDaysWithoutOrder
    # order by MaxDaysWithoutOrder desc
    # HINT: Use "WITH"; I created two CTE tables
    # HINT: Use Lag
    sql_statement = "with OrderedDates as (select distinct c.customerid, c.firstname, c.lastname, co.country, o.orderdate from orderdetail o join customer c on o.customerid = c.customerid join country co on c.countryid = co.countryid), LaggedDates as (select customerid, firstname, lastname, country, orderdate, lag(orderdate, 1) over (partition by customerid order by orderdate) as PreviousOrderDate from OrderedDates), Diffs as (select *, (julianday(orderdate) - julianday(PreviousOrderDate)) as DaysDiff from LaggedDates where PreviousOrderDate is not null), RankedDiffs as (select *, row_number() over (partition by CustomerID order by DaysDiff desc, OrderDate asc) as rn from Diffs) select CustomerID as CustomerID, FirstName as FirstName, LastName as LastName, Country as Country, OrderDate as OrderDate, PreviousOrderDate as PreviousOrderDate, DaysDiff as MaxDaysWithoutOrder from RankedDiffs where rn = 1 order by MaxDaysWithoutOrder desc, CustomerID desc"
    '''
    WITH OrderedDates AS (
            SELECT DISTINCT 
                C.CustomerID, 
                C.FirstName, 
                C.LastName, 
                Co.Country, 
                O.OrderDate 
            FROM OrderDetail O 
            JOIN Customer C ON O.CustomerID = C.CustomerID 
            JOIN Country Co ON C.CountryID = Co.CountryID
        ), 
        LaggedDates AS (
            SELECT 
                CustomerID, 
                FirstName, 
                LastName, 
                Country, 
                OrderDate, 
                LAG(OrderDate, 1) OVER (PARTITION BY CustomerID ORDER BY OrderDate) as PreviousOrderDate 
            FROM OrderedDates
        ), 
        CalculatedDiffs AS (
            SELECT 
                CustomerID, 
                FirstName, 
                LastName, 
                Country, 
                OrderDate, 
                PreviousOrderDate, 
                CAST((julianday(OrderDate) - julianday(PreviousOrderDate)) AS INTEGER) as DaysDiff 
            FROM LaggedDates 
            WHERE PreviousOrderDate IS NOT NULL
        ) 
        SELECT 
            CustomerID, 
            FirstName, 
            LastName, 
            Country, 
            OrderDate, 
            PreviousOrderDate, 
            MAX(DaysDiff) as MaxDaysWithoutOrder 
        FROM CalculatedDiffs 
        GROUP BY CustomerID 
        ORDER BY MaxDaysWithoutOrder DESC
    '''
    return sql_statement
