import glob                         # this module helps in selecting files
import pandas as pd                 # this module helps in processing CSV files
import xml.etree.ElementTree as ET  # this module helps in processing XML files.
from datetime import datetime

tmpfile    = "data"               # file used to store all extracted data
logfile    = "dealership_logfile.txt"            # all event logs will be stored in this file
targetfile = "dealership_transformed_data.csv"   # file where transformed data is stored


def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process,lines=True)
    return dataframe

def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=['car_model','year_of_manufacture','price', 'fuel'])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        car_model = person.find("car_model").text
        year_of_manufacture = int(person.find("year_of_manufacture").text)
        price = float(person.find("price").text)
        fuel = person.find("fuel").text
        dataframe = dataframe.append({"car_model":car_model, "year_of_manufacture":year_of_manufacture, "price":price, "fuel":fuel}, ignore_index=True)
    return dataframe


def extract():
    extracted_data = pd.DataFrame(columns=['car_model', 'year_of_manufacture', 'price',
                                           'fuel'])  # create an empty data frame to hold extracted data

    # process all csv files
    for csvfile in glob.glob("data/*.csv"):
        extracted_data = extracted_data.append(extract_from_csv(csvfile), ignore_index=True)

    # process all json files
    for jsonfile in glob.glob("data/*.json"):
        extracted_data = extracted_data.append(extract_from_json(jsonfile), ignore_index=True)

    # process all xml files
    for xmlfile in glob.glob("data/*.xml"):
        extracted_data = extracted_data.append(extract_from_xml(xmlfile), ignore_index=True)

    return extracted_data

def transform(data):
    data['price'] = round(data.price, 2)
    return data

def load(targetfile, data_to_load):
    data_to_load.to_csv(targetfile)

def log(message):
    timestamp_format = '%H:%M:%S-%h-%d-%Y' #Hour-Minute-Second-MonthName-Day-Year
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("dealership_logfile.txt","a") as f:
        f.write(timestamp + ',' + message + '\n')


# Log that you have started the ETL process
log('started the ETL process')

# Log that you have started the Extract step
log('started the Extract')
# Call the Extract function
data=extract()
# Log that you have completed the Extract step
log('completed the Extract')

# Log that you have started the Transform step
log('started the Transform')
# Call the Transform function
#transformed_data=transform(data)
# Log that you have completed the Transform step
log('completed the Transform')

# Log that you have started the Load step
log('started the Load')

# Call the Load function
load('ETLFIle.csv',data)
# Log that you have completed the Load step

log('completed the Load')
# Log that you have completed the ETL process
log('completed the ETL process')