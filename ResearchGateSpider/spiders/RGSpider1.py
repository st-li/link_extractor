# -*- coding: utf-8 -*-

from scrapy.spiders import CrawlSpider, Rule
from scrapy import Request, FormRequest
from scrapy.utils.request import request_fingerprint
from ResearchGateSpider.items import ResearchGateItem,  CandidateBasicItem
from ResearchGateSpider.datafilter import DataFilter
from ResearchGateSpider.func import parse_text_by_multi_content
from scrapy.exceptions import CloseSpider
from scrapy.linkextractors import LinkExtractor
import pymongo
import hashlib
import time
import re
import gzip
from w3lib.http import headers_dict_to_raw, headers_raw_to_dict

class RGSpider1(CrawlSpider):
    name = 'RGSpider1'
    college_name = 'George Washington University'
    college_id = '11'
    country_id = '1'
    state_id = '1'
    city_id = '1'
    allowed_domains = ['gwu.edu']
    start_urls = ['https://www.gwu.edu/']
    
    rules =(

        Rule(LinkExtractor(allow_domains=allowed_domains,
                           allow=('colleges-and-schools','schools_and_colleges','schools-colleges','content',r'[\w\-]+Faculty',r'academic([\-\w\d]+)','faculty',
                                  'people','profile','faculty-profiles','departments'
                                  ,'persons','faculty-staff','faculty_staff','faculty-profiles','faculty-directory',
                                  'dept','department-directory','person' ,r'/note/\d{4,5}$','profiles','directory'
                                  ),

                           deny=('our-people','committees','assembly','provost','governance',r'[\w\-]+honors',
                                 r'[\w\-]+fellowships','stories','sitemaintenance','blog','sitemap','pdf'
                                 r'[\w\-]+login',r'login[\w]+','photo',r'[\w\d\_]+\.dta',r'[\w]+\.do',
                                 r'[\w\_]+guide', r'[\w\-]+handbooks',r'[\w\d\_]+\.csv','appointments',
                                 'FAQs','administrators','publications','similar','fingerprints','network',
                                 r'[\w\-]+award',r'[\w\-]+awards',r'[\w\-]+mentors', r'[\w\-]+students',
                                 'meetings','alumni',r'[\w\-]+positions',
                                 r'[\w\d\_]+\.rtf','archive','positions',
                                 ),
                           tags=('a'),
                           attrs=('href'),
                           canonicalize=False,
                           unique=True,
                           ),
             process_links='link_filtering',
             callback='parse_item',
             follow=True
             ),
    )

    def link_filtering(self, links):
        pattern = re.compile('network|semi|lib|develop|alum|career|dean|lesson|undergrad|award|advis|publi|\bacademic\b',re.I)
        pattern1 = re.compile('class|calendar|journal|polic|job|pdf|\.doc|\.xls|admi|event|member|new|cv|course',re.I)
        pattern2 = re.compile('student|ensemble|login|office|camp|handbook|guide|degree|major|mentor|leadership',re.I)
        pattern3 = re.compile('contact|curriculum|stud|intern|program|meeting|fall|spring|cert|arch|ambass|faci|serv|tutor|proj',re.I)
        pattern4 = re.compile('graduate|diver|senate|center|counsel|emp|roll|utili|hr|manual|fund|ground|posts|messeng|appl',re.I)
        pattern5 = re.compile('home|conf|video|hosp|aid|hous|interv|surv|activ|agend|regist|help|announ|operat|image',re.I)
        pattern6 = re.compile('report|stand|secu',re.I)
        ret = []
        for link in links:

            if len(link.url) < 90 and pattern.findall(link.url) == [] and pattern1.findall(link.url) == [] and pattern2.findall(link.url) == [] \
                    and pattern3.findall(link.url) == [] and pattern4.findall(link.url) == [] and pattern5.findall(link.url) == [] \
                    and pattern6.findall(link.url) == []:
                ret.append(link)
        return ret

    def parse_item(self, response):
        if response.status in [x for x in range(400, 500) if x != 404]:
            # lostitem_str = 'The lost url is ' + response.url + '\n'
            # self.lostitem_file.write(lostitem_str)
            # self.lostitem_file.close()
            client = pymongo.MongoClient(
                "118.190.45.60",
                27017
            )
            db = client["link_extractor"]
            auth_result = db.authenticate(name='eol_spider', password='m~b4^Uurp)g', mechanism='SCRAM-SHA-1')
            collection = db['lost_url']
            collection.insert_one({'lost_usl':response.url, 'type':'overview'})
            client.close()
            raise CloseSpider(reason=u'被封了，准备切换ip')
        item = CandidateBasicItem()
        item['key'] = hashlib.sha256(response.url).hexdigest()
        item['country_id'] = self.country_id
        item['college_id'] = self.college_id
        item['discipline_id'] = '0'
        item['university'] = self.college_name

        item['url'] = response.url

        # response_headers = headers_dict_to_raw(response.headers)
        response_body = self._get_body(response.headers, response.body)

        item['source_code'] = response_body
        # item['source_text'] = parse_text_by_multi_content(response.xpath("//*"), '||||')
        item['header_title'] = response.xpath('//head/title/text()').extract()

        return item
        pass

    def __init__(self, **kwargs):
        super(RGSpider1, self).__init__(**kwargs)
        # self.lostitem_file = open('/data/lost_link_extractor.out', 'a+')
        pass

    def close(self, reason):
        # self.lostitem_file.close()
        super(RGSpider1, self).close(self, reason)

    @staticmethod
    def _get_body(headers, body):
        if "Content-Encoding" in headers and headers["Content-Encoding"] == "gzip":
            compressedstream = StringIO.StringIO(body)
            gzipper = gzip.GzipFile(fileobj=compressedstream)
            body = gzipper.read()
        else:
            body = body
        return body
