import json
from urllib.request import Request, urlopen


headers = {
    'Content-Type': 'text/plain',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36 Edg/88.0.705.50',
    'Referer': 'https://www.topuniversities.com/university-rankings/university-subject-rankings/2017/business-management-studies',
}

req = Request(
    url='https://www.topuniversities.com/sites/default/files/qs-rankings-data/335228.txt?_=1611539082436',
    headers=headers
)

res = urlopen(req)
print(res.headers)
data = res.read().decode('utf-8')
data = json.loads(data)
print(data['data'][:5])