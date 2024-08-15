import kiteconnect
import time
import platform
import subprocess
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs
import pickle

api_key         = "lstxlmmwrt85d63a"
api_secret      = "kkspf857dt510x856cvjemzx7yaw6e2b"

kite            = kiteconnect.KiteConnect(api_key=api_key)
login_url       = kite.login_url()

if platform.system() == 'Darwin':  # macOS
    subprocess.call(['osascript', '-e', 'open location "{}"'.format(login_url)])
else:
    webbrowser.open(login_url)

while True:
    time.sleep(1)
    current_url = subprocess.check_output(['osascript', '-e', 'tell application "Google Chrome" to return URL of active tab of front window']).strip().decode()
    if "request_token" in current_url:
        break

#print("curr_url = " , current_url)


parsed_url      = urlparse(current_url)
query_params    = parse_qs(parsed_url.query)

request_token   = query_params.get('request_token', [None])[0]
#print("request_token = " , request_token)

access_token    = kite.generate_session(request_token, api_secret)
access_token    = access_token['access_token']

tokenFile    = 'access_token.pkl'

with open(tokenFile, 'wb') as f:
    pickle.dump(access_token, f)

print("access_token is set = " , access_token)



