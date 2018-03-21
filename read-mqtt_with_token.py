import json, time, signal, sys, os, traceback,datetime
from contextlib import contextmanager
import jwt, requests, certifi
import paho.mqtt.client as mqtt
import numpy as np
import plotly.plotly as py
import plotly.tools as tls
import plotly.graph_objs as go

class DSH_API:
    def __init__(self, tenant, apikey, client_id, root):
        self.reqs = requests.Session()
        self.root = root
        self.tenant = tenant
        self.apikey = apikey
        self.client_id = client_id
        self.rest_token = None
        self.mqtt_token = None

    ### High level API
    def get_rest_token(self):
        if self.rest_token is None or self.is_token_expired( self.rest_token ):
            self.rest_token = self._fetch_rest_token()
        return self.rest_token
    
    def get_mqtt_token(self):
        if self.mqtt_token is None or self.is_token_expired( self.mqtt_token ):
            self.mqtt_token = self._fetch_mqtt_token( self.get_rest_token(), self.client_id )
        return self.mqtt_token

    def get_token_endpoint(self):
        decoded = jwt.decode(self.get_mqtt_token(), verify = False)
        return decoded['endpoint']

    def get_token_client_id(self):
        decoded = jwt.decode(self.get_mqtt_token(), verify = False)
        return decoded['client-id']

    def is_token_expired(self, token):
        decoded = jwt.decode(token, verify = False)
        exp = decoded['exp']
        return time.time() < exp

    ### low level API

    def get(self, path, params = {}, headers = {}):
        if not path.startswith('/'):
            path = '/' + path
        return self.reqs.get( self.root + path, params = params, headers = headers)

    def post(self, path, body, headers = {}):
        if not path.startswith('/'):
            path = '/' + path
        # see if the body is just a dictionary, 
        # and convert that to json
        if isinstance(body, dict):
            if not any( 'content-type' == k.lower() for k in headers ):
                headers['Content-Type'] = 'application/json'
                body = json.dumps(body)

        return self.reqs.post( self.root + path, data = body, headers = headers)

    def _fetch_rest_token(self):
        """
        Retrieve DSH REST token

        Makes an HTTP POST to <dsh>/auth/v0/token to retrieve the REST token
        The body is 
        { "tenant" : "...." }
        with Header apikey: <apikey>
        """
        headers = {"apikey": self.apikey}
        body = { "tenant" : self.tenant }
        resp = self.post( '/auth/v0/token', body = body, headers = headers  )
        print(resp)
        # raise an exception if not HTTP 200 OK

        resp.raise_for_status()
        # return body text - that is the token
        return resp.text

    def _fetch_mqtt_token(self, rest_token, client_id):
        """
        Retrieve DSH MQTT token

        Makes an HTTP POST to <dsh>/datastreams/v0/mqtt/token to retrieve the MQTT Token
        Needs a REST token to authenticate
        Retrieves full access token - no fine grained DSH ACL with claimes etc
        """
        body = {
            "tenant" : self.tenant,
            "id" : client_id
        }
        headers = {
            "Authorization" : "Bearer " + rest_token
        }
        resp = self.post('/datastreams/v0/mqtt/token', body = body, headers = headers)
        resp.raise_for_status()
        return resp.text


# process Ctrl-C and sigterm  so that mqtt client can disconnect nicely
@contextmanager
def auto_disconnect( mqtt_client ):
    # setup signal catchers
    sig_prev = {}
    catch_these = [ signal.SIGINT, signal.SIGTERM ]
    status = {
        "disconnected" : False
    }
    def on_sig(num, frame):
        prevsig = sig_prev[num]
        if num in catch_these:
            mqtt_client.disconnect()
            status['disconnected'] = True
            # if prevsig == signal.SIG_DFL or prevsig == signal.SIG_IGN or prevsig is None:
            #    sys.exit(0)
        if prevsig != signal.SIG_DFL and prevsig != signal.SIG_IGN and prevsig is not None:
            prevsig(num,frame)
    for s in catch_these :
        sig_prev[ s ] = signal.signal( s, on_sig )
    # run the with block
    yield mqtt_client
    # reset the signal catchers
    for s,v in sig_prev.items():
        signal.signal( s,v )
    # disconnect if not yet
    if not status['disconnected']:
        mqtt_client.disconnect()

global current_value
global s
global last

current_value = 0
last = 0
stream_ids = tls.get_credentials_file()['stream_ids']

# Get stream id from stream id list 
stream_id = stream_ids[0]

# Make instance of stream id object 
stream_1 = go.Stream(
    token=stream_id,  # link stream id to 'token' key
    maxpoints=80      # keep a max of 80 pts on screen
)

stream_1 = dict(token=stream_id, maxpoints=60)

# Initialize trace of streaming plot by embedding the unique stream_id
trace1 = go.Scatter(
    x=[],
    y=[],
    mode='lines+markers',
    stream=stream_1         # (!) embed stream id, 1 per trace
)

data = go.Data([trace1])

# Add title to layout object
layout = go.Layout(title='Beschikbare Parkeerplaatsen - P6 Eierenmarkt')

# Make a figure object
fig = go.Figure(data=data, layout=layout)

print ("Creating plot..")
# Send fig to Plotly, initialize streaming plot, open new tab
py.plot(fig, filename='python-streaming')

s = py.Stream(stream_id)

# We then open a connection
s.open()

# (*) Import module keep track and format current time

i = 0    # a counter
k = 5    # some shape parameter

# Delay start of stream by 5 sec (time to switch tabs)

    
    
Connected = False   #global variable for the state of the connection

# create the DSH API object 
# read environemnt variables for tenants, API KEYs etc, with some default values
BASE_POC_URL = 'https://api.poc.kpn-dsh.com'
MQTT_TOPIC = '/tt/poc-rotterdam-city-parking/#'

tenant = 'construct'
api_key = "ipS5GhqIDMLMo3LITNWV"
if api_key is None:
    print("Make sure you set API_KEY environment variable.\nWindows: set API_KEY=<...>\nLinux/Mac: export API_KEY=<...>")
    sys.exit(1)
client_id = os.getenv('DSH_CLIENT_ID','cur-parking-demo_1')
root_api_url = os.getenv('DSH_API_HOST',BASE_POC_URL)

def on_mqtt_connect(client,userdata,flags,rc):
    print("Connected to mqtt, trying to read the messages as they arrive")
    client.subscribe("/tt/poc-rotterdam-city-parking/#")
    client.publish("/tt/poc-rotterdam-city-parking/p6",json.dumps('{"parkingFacilityDynamicInformation": {"identifier": "test", "description": "", "name": "P6 - Eiermarkt", "facilityActualStatus": {"vacantSpaces": 190, "parkingCapacity": 365, "full": false, "open": true, "lastUpdated": 1516024040}}}'))

def on_mqtt_message(client,userdata,msg):
    print('topic {}, message {}'.format( msg.topic, msg.payload))
    x = json.loads(msg.payload)
    #if "parkingFacilityDynamicInformation" in x:
    #    current_value = x["parkingFacilityDynamicInformation"]["facilityActualStatus"]["vacantSpaces"]
    #    last = current_value
    #else:
    #    print (x)
    #    current_value = last
    current_value = 0 

    # Current time on x-axis, random numbers on y-axis
    x = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    y = int(current_value)
    # Send data to your plot
    s.write(dict(x=x, y=y))

def on_mqtt_publish(client,userdata,msg):
    print ("Publishing: " + str(userdata))

try:
    mqtt_client = None
    # create dsh api objecgt
    dshapi = DSH_API(tenant, api_key, client_id, root_api_url)
    # fetch all the tokens, ending with mqtt token. No exception - good
    mqtt_token = dshapi.get_mqtt_token()
    # get the client id from the token
    mqtt_client = mqtt.Client( dshapi.get_token_client_id() )
    # enable SSL/TLS connection mode
    mqtt_client.tls_set( certifi.where() )
    # set DSH authentication
    mqtt_client.username_pw_set('ignore', mqtt_token)
    # set the callbacks
    mqtt_client.on_connect = on_mqtt_connect
    mqtt_client.on_message = on_mqtt_message
    mqtt_client.on_publish = on_mqtt_publish
    x = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    last = 0
    s.write(dict(x=x, y=last))

    #mqtt_client.publish("/tt/poc-rotterdam-city-parking/p6",json.dumps('{"parkingFacilityDynamicInformation": {"identifier": "test", "description": "", "name": "P6 - Eiermarkt", "facilityActualStatus": {"vacantSpaces": 190, "parkingCapacity": 365, "full": false, "open": true, "lastUpdated": 1516024040}}}'))
    #while True:
       #     x = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            # Send data to your plot
      #      
       #     time.sleep(20)
            
    with auto_disconnect(mqtt_client):
        # connect 
        print("Trying to connect to DSH MQTT endpoint...")
        mqtt_client.connect( dshapi.get_token_endpoint(), 8883 )
        mqtt_client.loop_start()
        while True:
            mqtt_client.publish("/tt/poc-rotterdam-city-parking/p6",json.dumps('testestetste'))
            time.sleep(5)
        mqtt_client.loop_stop()

    
    
except :
    traceback.print_exc()
finally:
    if mqtt_client is not None:
        
        mqtt_client.disconnect()