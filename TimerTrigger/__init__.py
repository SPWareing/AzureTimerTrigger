import datetime
import logging
import requests
import azure.functions as func
import pandas as pd 
import re 
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta

def main(mytimer: func.TimerRequest, outputblob: func.Out[bytes]): 
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    
    #regex pattern to extract the dataset
    def regx(string):
        pattern = r'\D+\((\d)*\d\.\d{2} MB\)'
        compd = re.compile(pattern)
        mog = compd.search(string)
        return mog.group(0)
    
    # url to the dataset page
    uri = r"https://historicengland.org.uk/listing/the-list/data-downloads/"

    req = requests.get(uri)

    
    if mytimer.past_due:
        logging.info('The timer is past due!')
    
    try:
        req.raise_for_status()
        #Get the first table in the webpage
        df = pd.read_html(req.text)[0]
        #Strip the unecessary data
        df['Dataset (.zip)'] = df.apply(lambda x: regx(x['Dataset (.zip)']), axis=1)
        #change the Last updated column to datetime -- then sort
        df['Last updated'] = df['Last updated'].astype('datetime64')        
        df = df.sort_values('Last updated')

        df.reset_index(inplace=True, drop=True)
        
        #create a time delta of 1 month
        delta = relativedelta(months=1)
        check = dt.now() - delta

        # add the column Updated to the dataframe
        df['Updated'] = df.apply(lambda x: x['Last updated']>check, axis=1)
        
        logging.info(df.head())
        
        #get only the updated shapefiles
        df_updated = df[df.Updated==True]
        logging.info(df_updated.head())
        # save the csv string to a variable
        output = df_updated.to_csv()
        logging.info(output)
        logging.info("\nUploading to Azure Storage as blob")
        outputblob.set(output)
    except Exception as exc:
        logging.info('There was a problem: {}'.format(exc))
    
    


    logging.info('Python timer trigger function ran at %s', utc_timestamp)
