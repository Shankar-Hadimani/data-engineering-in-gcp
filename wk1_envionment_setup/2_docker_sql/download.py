import urllib.request
      
# download ny taxi parquet
url ="https://raw.githubusercontent.com/alagala/labs/master/azure/cosmosdb/graph/data/stations.snappy.parquet"
filename = 'stations.snappy.parquet'
urllib.request.urlretrieve(url, filename)