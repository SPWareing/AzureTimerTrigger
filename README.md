# Azure Timer Function to check for dataset update

This azure function uses the [Pandas](https://pandas.pydata.org/) library with the [Beautiful Soup](https://pypi.org/project/beautifulsoup4/) extension in order to check if any dataset has been updated on the [Historic England](https://historicengland.org.uk/listing/the-list/data-downloads/) listing website.

___


## How it Works

The NCRON expression for this reposoitory is set to run daily at 7am.
~~~

"schedule": "0 0 7 * * *"

~~~

This is **purely for testing**, and it is recommended that a longer expression be used when uploaded to Azure, for example *monthly*:
~~~
"schedule": 0 0 7 1 1-12 *
~~~

- When the timer is triggered it will send a `request.get()` call to the website. Upon a successful request code the `request.text` will be passed to a Pandas dataframe and processed. 
- A dataframe containing a reference to only the updated datasets will be output to Blob storage as a CSV.
- The output CSV is uniquely identified by appending a DateTime to the filename: `HE_event_{Datetime}.csv`
- For simplicity an Azure Logic App then checks the Blob storage for updates and then emails the results.