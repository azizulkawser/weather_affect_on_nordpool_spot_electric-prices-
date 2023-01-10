def transform_wind_directions_to_numeric(wind_direction):
    '''This function can be used in Exercise1 to transform compass-directions to numeric format'''

    wind_direction_numeric = []
    for item in wind_direction:
        if item == "N":
            wind_direction_numeric.append(0) #north is heading degree 0
        elif item == "NNE":
            wind_direction_numeric.append(22.5)
        elif item == "NE":
            wind_direction_numeric.append(45)
        elif item == "ENE":
            wind_direction_numeric.append(67.5)
        elif item == "E":
            wind_direction_numeric.append(90)
        elif item == "ESE":
            wind_direction_numeric.append(111.5)
        elif item == "SE":
            wind_direction_numeric.append(135)
        elif item == "SSE":
            wind_direction_numeric.append(157.5)
        elif item == "S":
            wind_direction_numeric.append(180)
        elif item == "SSW":
            wind_direction_numeric.append(202.5)
        elif item == "SW":
            wind_direction_numeric.append(225)
        elif item == "WSW":
            wind_direction_numeric.append(247.5)
        elif item == "W":
            wind_direction_numeric.append(270)
        elif item == "WNW":
            wind_direction_numeric.append(292.5)
        elif item == "NW":
            wind_direction_numeric.append(315)
        elif item == "NNW":
            wind_direction_numeric.append(337.5)
    return wind_direction_numeric

def parseHTMLfiles(data_folder):
    '''Parse HTML-files from the folder to get electricity prices into Dataframe in Exercise 2'''

    #imports for file and folder handling
    import pathlib 
    import pandas as pd

    #add "r" in front of folder-path so interpreter understands backslashes
    #data_folder = r"D:\DataEng\Electric_prices_data"

    date_list = []#list for storing dates
    prices_list = []#list for storing hourly prices
    prices_nested_list = []#list for storing dates and tables
    prices_dict = dict()

    for path in (pathlib.Path(data_folder).rglob('*')):# iterate through every sub-folder and file in the folder    
        #if path.is_file(): #if current path corresponds to file and not folder then enter
        if ".html" in str(path): #if there is html-file in path, then enter
            HtmlFile = open(path, 'r', encoding='utf-8')
            source_code = HtmlFile.read() 

            # read_html Returns list of all tables on page and in this case there is only one table
            # note that decimal points need to be correct in parsing
            table = pd.read_html(source_code, decimal=',', thousands='.')[0].values 
            
            if len(table)>0:#enter if list is not empty
                folder_name = path.parts[-2]#parse sub-folder name which includes date of current table
                current_date = folder_name.split('_tuntihinnat')[0]#parse date of folder name, assuming all folder names have same syntax
                current_date = current_date.replace('_', '-')#transform date string to format which Pandas understands DD-MM-YYYY
                for item in table:
                    #item is array where first element is string and split it to get first hour of corresponding price
                    
                    hour = str(item[0]).split('-')[0]
                    hour = int(hour)#+1 #add 1 because time format needs to be 1-24, not 0-23
                    prices_list.append(float(item[-1])) #append prices as float
                    current_date_time = current_date+'-'+str(hour) #create string in Dataframe Datetime form
                    date_list.append(current_date_time)

    prices_dict = {"Date":date_list, "Price":prices_list}#make dict of collected lists
    df = pd.DataFrame(prices_dict)
    df['Date'] = pd.to_datetime(df['Date'], format="%d-%m-%Y-%H", errors='ignore') #ignoring errors might cause data loss if formatting fails
    df = df.sort_values(by = 'Date')
    df.info()
    return df


def parseHTMLfilesColab(data_folder):
    '''Parse HTML-files from the folder to get electricity prices into Dataframe in Exercise 2, Colab-version with Google Drive folder'''

  #imports for file and folder handling
    import pathlib 
    import pandas as pd
    import glob

    #add "r" in front of folder-path so interpreter understands backslashes
    #data_folder = r"D:\DataEng\Electric_prices_data"

    date_list = []#list for storing dates
    prices_list = []#list for storing hourly prices
    prices_nested_list = []#list for storing dates and tables
    prices_dict = dict()

    for path in glob.glob(data_folder + '**/*.html'):
    #for path in (pathlib.Path(data_folder).rglob('*')):# iterate through every sub-folder and file in the folder    
        #if path.is_file(): #if current path corresponds to file and not folder then enter
        if ".html" in str(path): #if there is html-file in path, then enter
            HtmlFile = open(path, 'r', encoding='utf-8')
            source_code = HtmlFile.read() 

            # read_html Returns list of all tables on page and in this case there is only one table
            # note that decimal points need to be correct in parsing
            table = pd.read_html(source_code, decimal=',', thousands='.')[0].values 
            
            if len(table)>0:#enter if list is not empty
                try:
                    folder_name = path.parts[-2]#parse sub-folder name which includes date of current table
                except:
                    folder_name = path.split('/')[-2]
                current_date = folder_name.split('_tuntihinnat')[0]#parse date of folder name, assuming all folder names have same syntax
                current_date = current_date.replace('_', '-')#transform date string to format which Pandas understands DD-MM-YYYY
                for item in table:
                    #item is array where first element is string and split it to get first hour of corresponding price
                    
                    hour = str(item[0]).split('-')[0]
                    hour = int(hour)#+1 #add 1 because time format needs to be 1-24, not 0-23
                    prices_list.append(float(item[-1])) #append prices as float
                    current_date_time = current_date+'-'+str(hour) #create string in Dataframe Datetime form
                    date_list.append(current_date_time)

    prices_dict = {"Date":date_list, "Price":prices_list}#make dict of collected lists
    df = pd.DataFrame(prices_dict)
    df['Date'] = pd.to_datetime(df['Date'], format="%d-%m-%Y-%H", errors='ignore') #ignoring errors might cause data loss if formatting fails
    df = df.sort_values(by = 'Date')
    df.info()
        
    return df

def fetch_NCEI_weather_data(df):
    '''Obtain weather data from National Centers for Environmental Information
    Input Dataframe with "Date" column [DateTime], returns Dataframe with weather data off the same timeline
    #https://www.ncei.noaa.gov/support/access-data-service-api-user-documentation'''
    import requests
    import pandas as pd
    bb = '70,4,57,56'#format:[N, W, S, E], bounding box area of scandinavia and baltics
    #NCEI database allows only 1 month data request at time so need loop request through every month

    daily = df.groupby(pd.Grouper(key='Date', freq='D', sort=True))
    #print(monthly.head())
    weather_df_whole = pd.DataFrame()
    for data_group, df_group in daily:
        data=df_group    
    
        try:
            start_date = str(min(df_group['Date']))#format:2016-01-01
            start_date = start_date.split()[0]#split string into date only
            stop_date = str(max(df_group['Date']))
            stop_date = stop_date.split()[0]
            #Let's format url for request
            url = 'https://www.ncei.noaa.gov/access/services/data/v1?dataset=global-marine&dataTypes=WIND_DIR,WIND_SPEED&startDate='+start_date+'&endDate='+stop_date+'&boundingBox='+bb+'&units=metric'
            #r = requests.get(url)
            #open('temp.csv', 'wb').write(r.content)
            #weather_df = pd.read_csv('temp.csv')
            weather_df = pd.read_csv(url)
            
            #daily_windspeed_list.append(weather_df['WIND_SPEED'].dropna().mean()) #drop NaN values and take mean off hourly values of the date
            #dates_list.append(start_date)
            weather_df_whole = weather_df_whole.append(weather_df)
            print(weather_df_whole)
        except:
            print("error in url fetching")
    weather_df_whole.to_csv("weather_df.csv")

def create_connection_sqlite(path):
    import sqlite3
    from sqlite3 import Error

    connection = None
    try:
        connection = sqlite3.connect(path)
        print("Connection to SQLite DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")
    return connection

def execute_query_sqlite(connection, query):
    import sqlite3
    from sqlite3 import Error
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query executed successfully")
    except Error as e:
        print(f"The error '{e}' occurred")
