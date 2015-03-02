import copy
import os
import requests
import dataset
import urllib
from itertools import count
from urlparse import urljoin
from lxml import html
from datetime import datetime, timedelta
from slugify import slugify
from werkzeug.utils import secure_filename

try:
   from config import dburl
except ImportError:
   dburl = 'postgresql://localhost/sedar'
engine = dataset.connect(dburl)


#INDUSTRIES = '046,047,005,006,058,025'

INDUSTRIES_MINING = ','.join([
    # companies that have broken the water pitcher
    '046', # junior mining
    '001', #integrated mines
    '002', # metal mines
    '057', # mining
    '003', # non-base metal mining
])

INDUSTRIES_OIL= '047,005,006,058'

# mining
INDUSTRIES = INDUSTRIES_MINING
OUTPUT_DIR = '/data/sedar/mining_material_documents_2013'

TO_DATE = datetime(2014,1,1)
FROM_DATE = datetime(2013,1,1)

SEARCH_PAGE = 'http://www.sedar.com/search/search_form_pc_en.htm'
RESULT_PAGE = 'http://www.sedar.com/FindCompanyDocuments.do'
PARAMS = {
    'lang': 'EN',
    'page_no': 35,
    'company_search': 'All (or type a name)',
    'document_selection': 0,
    'industry_group': INDUSTRIES,
    'FromDate': FROM_DATE.strftime('%d'),
    'FromMonth': FROM_DATE.strftime('%m'),
    'FromYear': FROM_DATE.strftime('%Y'),
    'ToDate': TO_DATE.strftime('%d'),
    'ToMonth': TO_DATE.strftime('%m'),
    'ToYear': TO_DATE.strftime('%Y'),
    'Variable': 'DocType'
}

def scrape_many_years(last,first):
   global OUTPUT_DIR
   for year in range(last, first, -1):
      OUTPUT_DIR = '/data/sedar/mining_material_documents_%s' % year
      start = datetime(year,1,1)
      end = datetime(year+1,1,1)
      params = copy.copy(PARAMS)
      params.update({
            'FromDate': start.strftime('%d'),
            'FromMonth': start.strftime('%m'),
            'FromYear': start.strftime('%Y'),
            'ToDate': end.strftime('%d'),
            'ToMonth': end.strftime('%m'),
            'ToYear': end.strftime('%Y'),
            })
      print('---scraping year %s' % year)
      print(params)
      load_filings(params)      
   

print PARAMS
filing = engine['filing']
company = engine['company']
filing_index = engine['filing_index']
sess = {}


def chomp_name(key):
    return slugify(key).replace('-', '_').strip('_')


def get_industries():
    res = requests.get(SEARCH_PAGE)
    doc = html.fromstring(res.content)
    for opt in doc.findall('.//select[@name="industry_group"]/option'):
        print opt.get('value'), ' -> ', opt.text_content().strip()


def get_company(url):
    if company.find_one(url=url):
        return
    res = requests.get(url)
    doc = html.fromstring(res.content)
    content = doc.find('.//div[@id="content"]')
    data = {'url': url, 'name': content.findtext('.//td/font/strong')}
    print 'Company', [data['name']]
    key = None
    for row in content.findall('.//td'):
        if row.get('class') == 'bt':
            key = chomp_name(row.text)
        elif row.get('class') == 'rt' and key is not None:
            data[key] = row.text
            key = None
        #print url, html.tostring(row)
    company.upsert(data, ['url'])


def download_document(form):
    file_name = form.split('/filings/', 1)[-1]
    filing_id, doc_id, rest = file_name.split('/', 2)
    rest = secure_filename(urllib.unquote(rest))
    file_name = os.path.join(OUTPUT_DIR, filing_id, doc_id, rest)
    if os.path.exists(file_name):
        return file_name

    from breaker import make_cracked_session
    print "Downloading", [form]
    if 'ca' not in sess or sess['ca'] is None:
        sess['ca'] = make_cracked_session()

    res = sess['ca'].get(form)
    if 'x-powered-by' in res.headers or \
            'Accept Terms of Use' in res.content:
        sess['ca'] = make_cracked_session()
        return download_document(form)

    try:
        os.makedirs(os.path.dirname(file_name))
    except:
        pass

    with open(file_name, 'wb') as fh:
        fh.write(res.content)

    return file_name

def should_download_this(filingtype):
   '''
   only download docs that are material filings, material documents, or similar
   '''
   ftype = filingtype.lower()
   if 'material' in ftype:
      if ('document' in ftype) or ('contract' in ftype) or ('incorporated by reference' in ftype):
         print('filing to download: %s' % ftype)
         return True
   print('filing not to download: %s' % ftype)
   return False

def load_filings(global_params):
    status = 'SCROLLING'
    skiplength = 1
    i = 1
    while True:
        page_hits = 0
        params = global_params.copy()
        params['page_no'] = i
        print('on page %s' % i)
        res = requests.get(RESULT_PAGE, params=params)
        doc = html.fromstring(res.content)
        for row in doc.findall('.//tr'):
            cells = row.findall('.//td')
            if len(cells) < 6:
                continue
            submit = cells[3].getchildren()[0].get('action')
            #print cells, html.tostring(row)
            if submit is None:
                continue
            filing_id = submit.split('fileName=', 1)[-1]
            print 'Filing', [filing_id]
            form = urljoin(RESULT_PAGE, submit)
            page_hits += 1
            filing_type = cells[3].text_content().strip()
            data = {
                'filing': filing_id,
                'company': cells[0].text_content().strip(),
                'company_url': urljoin(RESULT_PAGE, cells[0].find('./a').get('href')),
                'date': cells[1].text_content().strip(),
                'time': cells[2].text_content().strip(),
                'type': cells[3].text_content().strip(),
                'tos_form': form,
                'format': cells[4].text_content().strip(),
                'size': cells[5].text_content().strip()
            }
            filing_index.upsert(data, ['filing'])            
            if not should_download_this(filing_type):
               continue

            if status == 'SCROLLING':
               status = 'DOWNLOADING'
               print('\n\n\nFOUND FIRST USEFUL DOC\n\n')
               # we've hit the first relevant docs; scroll back to the start
               i -= skiplength

            file_name = download_document(form)
            data = {
                'filing': filing_id,
                'file_name': file_name,
                'company': cells[0].text_content().strip(),
                'company_url': urljoin(RESULT_PAGE, cells[0].find('./a').get('href')),
                'date': cells[1].text_content().strip(),
                'time': cells[2].text_content().strip(),
                'type': cells[3].text_content().strip(),
                'tos_form': form,
                'format': cells[4].text_content().strip(),
                'size': cells[5].text_content().strip()
            }
            filing.upsert(data, ['filing'])
            get_company(data['company_url'])
            print('downloaded filing')

        if status == 'SCROLLING':
           i += skiplength
        else:
           i += 1

        if page_hits == 0:
            return

if __name__ == '__main__':
   scrape_many_years(2000,1990)

