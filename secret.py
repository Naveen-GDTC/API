import hvac

client = hvac.Client(url='http://192.168.1.195:8200', token='token')
res = client.is_authenticated()
read_secret_result  = client.read('secret/data/cred')

location =read_secret_result['data']['data']

API_KEY=location['API_KEY']
PORT=location['PORT']
POST_USER=location['POST_USER']
POST_PASS=location['POST_PASS']
DB=location['DB']
HOST=location['HOST']


