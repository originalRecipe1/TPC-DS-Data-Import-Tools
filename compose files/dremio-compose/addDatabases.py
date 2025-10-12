import json
import requests
import time
import copy

username = 'admin'
password = 'qwertz123456'
dremioServer = 'http://localhost:9047'
headers = {'content-type':'application/json'}

def apiGet(endpoint):
  return json.loads(requests.get('{server}/{endpoint}'.format(server=dremioServer, endpoint=endpoint), headers=headers).text)

def apiPost(endpoint, body=None):
  text = requests.post('{server}/{endpoint}'.format(server=dremioServer, endpoint=endpoint), headers=headers, data=json.dumps(body)).text

  if (text):
    return json.loads(text)
  else:
    return None # Future: return response code

def apiPut(endpoint, body=None):
  return requests.put('{server}/{endpoint}'.format(server=dremioServer, endpoint=endpoint), headers=headers, data=json.dumps(body)).text

def apiDelete(endpoint):
  return requests.delete('{server}/{endpoint}'.format(server=dremioServer, endpoint=endpoint), headers=headers)

def login(username, password):
  loginData = {'userName': username, 'password': password}
  response = requests.post('{server}/apiv2/login'.format(server=dremioServer), headers=headers, data=json.dumps(loginData))
  data = json.loads(response.text)

  token = data['token']
  return {'content-type':'application/json', 'authorization':'_dremio{authToken}'.format(authToken=token)}


register_body = {
    "userName":"admin",
    "firstName":"admin",
    "lastName":"admin",
    "email":"admin@admin.com",
    "createdAt":int(time.time()*1000),
    "password":"qwertz123456",
    "extra":None
    }

registered = apiPut('apiv2/bootstrap/firstuser', register_body)
#print(registered)
headers = login(username, password)
#print(headers)
postgres = {
    "config": {
        "username": "postgres",
        "password": "123456",
        "hostname": "192.168.178.113",
        "port": "5439",
        "databaseName": "db1",
        "useSsl": False,
        "authenticationType": "MASTER",
        "fetchSize": 200,
        "maxIdleConns": 8,
        "idleTimeSec": 60,
        "encryptionValidationMode": "CERTIFICATE_AND_HOSTNAME_VALIDATION",
        "propertyList": []
    },
    "name": "postgres",
    "accelerationRefreshPeriod": 3600000,
    "accelerationGracePeriod": 10800000,
    "metadataPolicy": {
        "deleteUnavailableDatasets": True,
        "namesRefreshMillis": 3600000,
        "datasetDefinitionRefreshAfterMillis": 3600000,
        "datasetDefinitionExpireAfterMillis": 10800000,
        "authTTLMillis": 86400000,
        "updateMode": "PREFETCH_QUERIED"
    },
    "type": "POSTGRES",
    "accessControlList": {
        "userControls": [],
        "roleControls": []
    }
}

postgres2 = copy.deepcopy(postgres)
postgres2["config"]["port"] = "5440"

postgres3 = copy.deepcopy(postgres)
postgres3["config"]["port"] = "5441"

print(apiPut('apiv2/source/postgres1', postgres))
print(apiPut('apiv2/source/postgres2', postgres2))
print(apiPut('apiv2/source/postgres3', postgres3))

mysql = {"config":{"username":"mysql","password":"123456","hostname":"172.17.0.1","port":"3306","database":"","authenticationType":"MASTER","netWriteTimeout":60,"fetchSize":200,"maxIdleConns":8,"idleTimeSec":60,"propertyList":[{}]},"name":"mysql","accelerationRefreshPeriod":3600000,"accelerationGracePeriod":10800000,"metadataPolicy":{"deleteUnavailableDatasets":True,"namesRefreshMillis":3600000,"datasetDefinitionRefreshAfterMillis":3600000,"datasetDefinitionExpireAfterMillis":10800000,"authTTLMillis":86400000,"updateMode":"PREFETCH_QUERIED"},"type":"MYSQL","accessControlList":{"userControls":[],"roleControls":[]}}

#print(apiPut('apiv2/source/mysql_db', mysql))

maria = {"config":{"username":"mariadb","password":"123456","hostname":"172.17.0.1","port":"3307","database":"","authenticationType":"MASTER","netWriteTimeout":60,"fetchSize":200,"maxIdleConns":8,"idleTimeSec":60,"propertyList":[{}]},"name":"mariadb","accelerationRefreshPeriod":3600000,"accelerationGracePeriod":10800000,"metadataPolicy":{"deleteUnavailableDatasets":True,"namesRefreshMillis":3600000,"datasetDefinitionRefreshAfterMillis":3600000,"datasetDefinitionExpireAfterMillis":10800000,"authTTLMillis":86400000,"updateMode":"PREFETCH_QUERIED"},"type":"MYSQL","accessControlList":{"userControls":[],"roleControls":[]}}

#print(apiPut('apiv2/source/mariadb', maria))