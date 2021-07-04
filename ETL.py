#Import statements
from pyspark.sql.functions import from_unixtime, substring, isnan, when, count, col, coalesce, datediff, to_date, udf, mean, avg
from pyspark.sql.types import TimestampType
from pyspark.sql import SparkSession
import sys

#Constants
days21 = "2018-05-07"
days15 = "2018-05-13"
days7 = "2018-05-21"
columnsList = ["UserID",
                "No_of_days_Visited_7_Days",
                "No_Of_Products_Viewed_15_Days",
                "User_Vintage",
                "Most_Viewed_product_15_Days",
                "Most_Active_OS",
                "Recently_Viewed_Product",
                "Pageloads_last_7_days",
                "Clicks_last_7_days"]

#Helper functions
def os_clean(os):
    if os=="android":
        return "Android"
    elif os=="windows":
        return "Windows"
    elif os=="mac os x":
        return "Mac OS X"
    elif os=="ios":
        return "iOS"
    elif os=="linux":
        return "Linux"
    elif os=="ubuntu":
        return "Ubuntu"
    elif os=="chrome os":
        return "Chrome OS"
    else:
        return os

def pr_clean(pr):
    if pr[0].islower():
        return pr[0].upper() + pr[1:]
    else:
        return pr

def act_clean(act):
    if (act != act) | (act is None):
        return None
    if act[0].islower():
        return act[0].upper()+act[1:]
    else:
        return act[0]+act[1:].lower()

def stringToTimestamp(dataframe, column):
    dataframe = dataframe.withColumn(column, dataframe[column].cast(TimestampType()))
    return dataframe

def showTableAndSchema(dataframe):
    dataframe.show(truncate=False)
    dataframe.printSchema()

def showNullCount(dataframe):
    dataframe.select([count(when(col(c).isNull(), c)).alias(c) for c in dataframe.columns]).show()

#Code

#Create SparkSession
spark = SparkSession.builder.getOrCreate()

#Read the datasets into spark dataframes
df_userTable = spark.read.format('org.apache.spark.sql.execution.datasources.csv.CSVFileFormat').option('header', 'true').load(sys.argv[1])
df_userTable.take(5)
df_visitorLogsData = spark.read.format('org.apache.spark.sql.execution.datasources.csv.CSVFileFormat').option('header', 'true').load(sys.argv[2])
df_visitorLogsData.take(5)

#Quick look at the userTable dataframe
showTableAndSchema(df_usertable)

#Since the Signup Date is in string format, let's convert it to Timestamp format
df_userTable = stringToTimestamp(df_userTable, "Signup Date")

#Check for null values in the dataframe per column
showNullCount(df_userTable)

#Convert this dataframe to an SQL table
df_userTable.createOrReplaceTempView('table1')

#Quick look at the visitorLogsData dataframe
showTableAndSchema(df_visitorLogsData)

#Check for null values in the dataframe per column
showNullCount(df_visitorLogsData)

#Convert both datetime formats to Timestamp format
df_visitorLogsData = (df_visitorLogsData
    .withColumn("VisitDateTime", coalesce(from_unixtime(substring("VisitDateTime", 0, 10), format='yyyy-MM-dd HH:mm:ss.SSS'), df_visitorLogsData['VisitDateTime'].cast(TimestampType()))
    .cast(TimestampType())))

#Convert this dataframe to an SQL table
df_visitorLogsData.createOrReplaceTempView('table2')

#Clean table2
#Filter in only the records from last 21 days
df_2 = spark.sql("SELECT * FROM table2 WHERE VisitDateTime >= '" + days21 + ""'")

#Update table2
df_2.createOrReplaceTempView('table2')

#Filter out records that do not contain UserID
df_2 = spark.sql("SELECT * FROM table2 WHERE UserID IS NOT NULL")

#Define user-defined functions for cleaning
osClean = udf(lambda x: os_clean(x))
prClean = udf(lambda x: pr_clean(x))
actClean = udf(lambda x: act_clean(x))

#Apply the user-defined functions for cleaning
df_2 = df_2.withColumn('OS', osClean('OS'))
df_2 = df_2.withColumn('ProductID', prClean('ProductID'))
df_2 = df_2.withColumn('Activity', actClean('Activity'))

#Update table2
df_2.createOrReplaceTempView('table2')

#Input features construction
#Create User_Vintage column by subtracting Signup Date from current date (28-05-2018)
User_Vintage_df = spark.sql('SELECT `UserID`, DATEDIFF("2018-05-28 00:00:00", `Signup Date`) AS `User_Vintage` FROM table1 ORDER BY UserID ASC')
#User_Vintage_df.show(truncate=False)

#Create No_Of_Products_Viewed_15_Days column by counting unique ProductIDs per UserID for the last 15 days
Products_15_df = spark.sql('SELECT UserID, COUNT(DISTINCT ProductID) AS `No_Of_Products_Viewed_15_Days` FROM table2 WHERE VisitDateTime >= "' + days15 + '" AND ProductID IS NOT NULL GROUP BY UserID ORDER BY UserID')
#Products_15_df.show(truncate=False)

#Create Pageloads_last_7_days column by counting number of records having Activity as Pageload per UserID in the last 7 days
Pageloads_7_df = spark.sql('SELECT UserID, COUNT(Activity) AS `Pageloads_last_7_days` FROM table2 WHERE VisitDateTime >= "' + days7 + '" AND Activity = "Pageload" GROUP BY UserID ORDER BY UserID')
#Pageloads_7_df.show(truncate=False)

#Create Clicks_last_7_days column by counting number of records having Activity as Click per UserID in the last 7 days
Clicks_7_df = spark.sql('SELECT UserID, COUNT(Activity) AS `Clicks_last_7_days` FROM table2 WHERE VisitDateTime >= "' + days7 + '" AND Activity = "Click" GROUP BY UserID ORDER BY UserID')
#Clicks_7_df.show(truncate=False)

#Create No_of_days_Visited_7_Days column by counting number of days in the last 7 days when the user was active
#Convert datetime to date only format for facilitating the counting procedure
Visited_7_df = df_2.withColumn("DateOnly", to_date(col("VisitDateTime")))
Visited_7_df.createOrReplaceTempView('table2_dateOnly')
Visited_7_df = spark.sql('SELECT UserID, COUNT(DISTINCT DateOnly) AS `No_of_days_Visited_7_Days` FROM table2_dateOnly WHERE DateOnly >= "' + days7 + '" GROUP BY UserID ORDER BY UserID')
#Visited_7_df.show(truncate=False)

#Create Recently_Viewed_Product column by filtering in records with Pageloads sorted by the VisitDateTime to pick the most recent ProductID
Recent_prod_df = spark.sql('SELECT * FROM table2 WHERE Activity = "Pageload" ORDER BY VisitDateTime DESC')
Recent_prod_df.createOrReplaceTempView('table2_pageload_sortedDesc')
Recent_prod_df = spark.sql('SELECT UserID, FIRST(ProductID) AS Recently_Viewed_Product FROM table2_pageload_sortedDesc WHERE ProductID IS NOT NULL GROUP BY UserID ORDER BY UserID')
#Recent_prod_df.show(truncate=False)

#Create Most_Active_OS column by counting the OSs used by the user and picking the one with the highest count
Most_OS_df = spark.sql('SELECT UserID, OS, COUNT(*) AS Count FROM table2 GROUP BY OS, UserID ORDER BY UserID ASC, Count DESC')
Most_OS_df.createOrReplaceTempView('table2_osCount_sortDesc')
Most_OS_df = spark.sql('SELECT UserID, FIRST(OS) AS Most_Active_OS FROM table2_osCount_sortDesc WHERE OS IS NOT NULL GROUP BY UserID ORDER BY UserID')
#Most_OS_df.show(truncate=False)

#Create Most_Viewed_product_15_Days column by counting the ProductIDs viewed by the user and picking the one with the highest count in the last 15 days
Most_15_df = spark.sql('SELECT UserID, ProductID, COUNT(*) AS Count FROM table2_pageload_sortedDesc WHERE VisitDateTime >= "' + days15 + '" GROUP BY ProductID, UserID ORDER BY UserID ASC, Count DESC')
Most_15_df.createOrReplaceTempView('table2_last15_prodCount_sortDesc')
Most_15_df = spark.sql('SELECT UserID, FIRST(ProductID) AS Most_Viewed_product_15_Days FROM table2_last15_prodCount_sortDesc WHERE ProductID IS NOT NULL GROUP BY UserID ORDER BY UserID')
#Most_15_df.show(truncate=False)

#Create an array of all the sub-dataframes
dfs = [User_Vintage_df, Products_15_df, Pageloads_7_df, Clicks_7_df, Visited_7_df, Recent_prod_df, Most_OS_df, Most_15_df]

#Join all these sub-dataframes to form the final dataframe
final_df = User_Vintage_df
for i in range(1, len(dfs)):
    #Use left join starting with the User_Vintage dataframe since it contains the records for all the UserIDs
    final_df = final_df.join(dfs[i], "UserID", "left")

#Impute null values in the dataframe accordingly
final_df = final_df.na.fill(0, ["No_Of_Products_Viewed_15_Days", "Pageloads_last_7_days", "Clicks_last_7_days", "No_of_days_Visited_7_Days"]) \
                   .na.fill("Product101", ["Recently_Viewed_Product", "Most_Viewed_product_15_Days"]) \
                   .na.fill("Windows", ["Most_Active_OS"])

#Sort the columns and the records as per the required format
final_df = final_df.select(columnsList)
final_df = final_df.sort("UserID")

#Save the final input dataframe to the present working directory as solution.csv
project.save_data(file_name = "solution.csv", data = final_df.toPandas().to_csv(index=False), overwrite=True, set_project_asset = True)
