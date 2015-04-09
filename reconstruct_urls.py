'''
Figure out original URLs for sedar docs, based on our
stored versions
'''

import dataset
import re
import sys

try:
   from config import dburl
except ImportError:
   dburl = 'postgresql://localhost/sedar'
engine = dataset.connect(dburl)

def fn_from_url(url):
    'https://sedar.openoil.net.s3.amazonaws.com/oil/oil_material_documents_2013/02089691/00000001/sPGLSuroco2013materialdoc073013.pdf'
    return re.search

def fromurl(url):
    #baseurl = 'http://sedar.com/GetFile.do?lang=EN&docClass=13&issuerNo=00010658&fileName=/csfsprod/data150/filings/02293790/00000001/k%3A%5CsClerks%5Cwpdata%5C01%5Ccondis%5C2015%5Cannuals%5C01-Q4ceo%28Oct31-14%29.pdf'
    searchpattern = re.search('documents_\d+(/\d+/\d+/)', url).group(1)
    query = "select tos_form from filing where filing like :like_query"
    like_query = '%%%s%%' % searchpattern
    return list(engine.query(query, like_query=like_query))[0]['tos_form']

if __name__ == '__main__':
    for line in sys.stdin:
        try:
            print(fromurl(line.strip()))
        except Exception:
            print 'UNKNOWN'
