import datetime
import logging
import requests
import azure.functions as func
import pandas as pd 
import bs4 
import re 
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta

def main(mytimer: func.TimerRequest, outputblob: func.Out[bytes]): 
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    
    #regex pattern to extract the Historic England dataset
    def regx_eng(string):
        pattern = r'\D+\((\d)*\d\.\d{2} MB\)'
        compd = re.compile(pattern)
        mog = compd.search(string)
        return mog.group(0)
    #regex pattern for the Historic Scotland dataset
    def regx_scot(string):
        pattern = r'(?<=Dataset: )\D+'
        compd = re.compile(pattern)
        mog = compd.search(string)
        return mog.group(0)

    #Time delta 
    delta = relativedelta(weeks=2)
    #create a time delta of 2 weeks        
    check = dt.now() - delta
    # url to the dataset page
    uri = r"https://historicengland.org.uk/listing/the-list/data-downloads/"

    
    # urls to the Historic Scotland datasets
    uris = [r"https://data.gov.uk/dataset/722b93f3-75fd-47ce-9f06-0efcfa010ecf/listed-buildings", 
    r'https://data.gov.uk/dataset/9075113f-d8e3-48da-bbfc-34f58939529b/scheduled-monuments',
    r'https://data.gov.uk/dataset/433101a0-3bd3-4035-b028-ea8d7a11abfd/conservation-areas',
    r'https://data.gov.uk/dataset/eab6ee72-23e8-46df-b74b-c2a9cb3ee6e0/world-heritage-sites',
    r'https://data.gov.uk/dataset/bab10bd8-cc8b-4b4e-9eb7-d620a3ee27d9/gardens-and-designed-landscapes',
    r'https://data.gov.uk/dataset/e290e0b9-b85e-4c9a-a1a3-475acacf5dfe/battlefields-inventory-boundaries',
    r'https://data.gov.uk/dataset/484afc0c-2b62-4218-a464-ef32a1a60a69/historic-marine-protected-areas']
    
    #urls to the Cadw datasets
    cadw = [r"https://datamap.gov.wales/layers/inspire-wg:Cadw_DesignatedWrecks",
     r"https://datamap.gov.wales/layers/inspire-wg:Cadw_ListedBuildings",
     r'https://datamap.gov.wales/layers/inspire-wg:Cadw_HistoricLandscapes',
     r'https://datamap.gov.wales/layers/inspire-wg:Cadw_SAM',
     r"https://datamap.gov.wales/layers/geonode:GWC21_World_Heritage_Site",
     r"https://datamap.gov.wales/layers/inspire-wg:conservation_areas"]

    if mytimer.past_due:
        logging.info('The timer is past due!')
        
    req = requests.get(uri)
    try:
        req.raise_for_status()
        
        #create a BeautfulSoup Object
        soup = bs4.BeautifulSoup(req.text, 'html.parser')
        table = soup.find('table',{'class': 'download-table'})
        #Get the modal links for the shapefiles
        links = []
        for tr in table.findAll("tr"):
            trs = tr.findAll("td", {'class':"download-table__cell download-table__cell--modal-link"})
            for each in trs:
                try:
                    link = each.find('a')['href']
                    links.append(uri+link)
                except:
                    pass
        #Get the first table in the webpage        
        df = pd.read_html(req.text)[0]
        df['source'] = links
        #Strip the unecessary data
        df['Dataset (.zip)'] = df.apply(lambda x: regx_eng(x['Dataset (.zip)']), axis=1)
        #change the Last updated column to datetime -- then sort
        df['Last updated'] = df['Last updated'].astype('datetime64')        
        
        # add the columns updated, organisation to the dataframe
        df = df.assign(updated=lambda x: x['Last updated']>check,
            organisation='Historic England')
        
        df = df.sort_values('Last updated')

        df.reset_index(inplace=True, drop=True)     
                
        
        df.rename(columns={'Dataset (.zip)': 'dataset', 'Last updated': 'last_update'}, inplace=True)
        #reorder the columns
        df = df.iloc[:, [6,0,2,4,5]]

        logging.info('The length of the HE dataset is {}'.format(len(df)))
        
        
        
        
    except Exception as exc:
        logging.info('There was a problem with the HE dataset: {}'.format(exc))
    
    
    heritage = pd.DataFrame()

    for uri in uris:
        req = requests.get(uri)
        try:
            req.raise_for_status()
            dfs = pd.read_html(req.text)[0]
            soup = bs4.BeautifulSoup(req.text, 'html.parser')
            table = soup.find('table')

            links = []
            for tr in table.findAll("tr"):
                    trs = tr.findAll("td")
                    for each in trs:
                        try:
                            link = each.find('a')['href']
                            links.append(link)
                        except:
                            pass

            dfs['source'] = links
            heritage = heritage.append(dfs)              
        except Exception as e:
            logging.info(f"There was a problem with the Historic Scotland Dataset{e}")   

    if len(heritage)>0:        
        heritage = heritage[heritage['Format']=='ZIP']
        logging.info(f'The length of the Historic Scotland dataset is: {len(heritage)}')
        heritage['last_update'] = heritage['File added'].astype('datetime64')
        heritage.drop(columns=['File added'], inplace=True)
        heritage['dataset'] = heritage.apply(lambda x: regx_scot(x['Link to the data']), axis=1)
                
        heritage = heritage.assign(updated = lambda x: x['last_update']> check,
                organisation ='Historic Scotland')
        last_column = heritage.pop('dataset')
        heritage.insert(0, 'dataset', last_column)
        

        


        #reorder the columns
        heritage = heritage.iloc[:,[7,0,5,4,6]]
        combined = df.append(heritage)
        logging.info(f'The length of the Combined dataset is: {len(combined)}')
        
    
    else:
        logging.info('The Heritage dataset was empty')
        combined = df

    cadws = pd.DataFrame()
    for ur in cadw:
        req = requests.get(ur)
        try:
            req.raise_for_status()
            soup = bs4.BeautifulSoup(req.text, 'html.parser')
            table = soup.find('dl')
            # table headers
            heads= [t.text.strip(':') for t in table.find_all('dt')]
            #table body
            body = [i.text.strip().replace('\n','') for i in  table.find_all('dd') if not table.find_all('dd').index(i) in (2,5) ]
            #create a dictionary object
            data =dict(zip(heads, body))
            #get the name of the dataset                        
            data['dataset'] = [h.text for h in soup.findAll('h2', {'class':"page-title"})][0]
            data['source'] = ur
            #rename the keys
            if 'Creation date' in data.keys():
                data['Publication date'] = data.pop('Creation date')
            

            cadws = cadws.append(data, ignore_index=True)

        
        except Exception as e:
            logging.info(f'There was a problem: {e} \n with the {ur}') 

    if len(cadws)> 0:
        logging.info(f'The length of the Cadw dataset is: {len(cadws)}')
        cadws.drop(columns=['Keywords', 'Point of contact', 'License', 'Category', 'Type'], inplace= True)
        popped = cadws.pop('dataset')
        cadws.insert(0, 'dataset', popped)
        cadws.insert(0, 'organisation', 'Cadw')
        cadws = cadws.assign(last_update = cadws['Publication date'].astype('datetime64'),
            updated = lambda x: x.last_update > check)

        cadws.pop('Publication date')
        combined = combined.append(cadws)
        logging.info(f'The length of all datasets is: {len(combined)}')
    else:
        logging.info('The Cadws dataset was empty')

    combined.reset_index(inplace=True, drop=True)
    #get only the updated shapefiles
    df_updated = combined[combined['updated']==True]
    logging.info('The size of the Updates dataset is {}'.format(len(df_updated)))
    if len(df_updated)>0:
        # save the csv string to a variable
        output = df_updated.to_csv()
    else:
        output = 'No Datasets have been updated in the last two weeks'
    logging.info(df_updated.head())
    
    
    logging.info(output)

    logging.info("\nUploading to Azure Storage as blob")
    outputblob.set(output)    

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
