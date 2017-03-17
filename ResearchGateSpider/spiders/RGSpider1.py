# -*- coding: utf-8 -*-

from scrapy.spiders import CrawlSpider, Rule
from scrapy import Request, FormRequest
from scrapy.utils.request import request_fingerprint
from ResearchGateSpider.items import ResearchGateItem,  CandidateBasicItem
from ResearchGateSpider.datafilter import DataFilter
from ResearchGateSpider.func import parse_text_by_multi_content
from scrapy.exceptions import CloseSpider
from scrapy.linkextractors import LinkExtractor
import scrapy
import pymongo
import hashlib
import time
import re
import gzip
from w3lib.http import headers_dict_to_raw, headers_raw_to_dict
#import pandas as pd

class RGSpider1(CrawlSpider):
    name = 'RGSpider1'
    # college_name = 'University of Connecticu'
    college_id = '11'
    country_id = '1'
    state_id = '1'
    city_id = '1'
    # university_df = pd.read_csv('e:\\work\\link_extractor\\university_list.csv', header=None)
    # university_df.columns = ['url', 'name', 'domain']
    # university_list = dict(zip(university_df['url'].tolist(), university_df['name'].tolist()))
    # allowed_domains = university_df['domain'].tolist()
    # start_urls = university_df['url'].tolist()
    # print start_urls
    # time.sleep(3)
    # print allowed_domains

    
    rules =(
        Rule(LinkExtractor(
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
        pattern1 = re.compile('class|calendar|journal|polic|job|pdf|\.doc|\.xls|admi|event|member|new|cv|course|\.gz|\.tar|\.max',re.I)
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

        item = CandidateBasicItem()
        item['key'] = hashlib.sha256(response.url).hexdigest()
        item['country_id'] = self.country_id
        item['college_id'] = self.college_id
        item['discipline_id'] = '0'
        item['university'] = self.university
        item['url'] = response.url

        # response_headers = headers_dict_to_raw(response.headers)
        item['header_title'] = response.xpath('//head/title/text()').extract()
        response_body = self._get_body(response.headers, response.body)
        item['source_code'] = response_body
        
#        print type(response)
#        # item['source_text'] = parse_text_by_multi_content(response.xpath("//*"), '||||')
#        if not isinstance(response, scrapy.http.response.Response):
#            item['header_title'] = response.xpath('//head/title/text()').extract()
#            response_body = self._get_body(response.headers, response.body)
#            item['source_code'] = response_body
#        else:
#            item['header_title'] = ''
#            item['source_code'] = ''
        return item
        pass

    def __init__(self, start_url = '', domain = '', university = '', **kwargs):
        super(RGSpider1, self).__init__(**kwargs)
        self.start_urls = [start_url]
        self.allowed_domains = [domain]
        self.university = university
        # self.lostitem_file = open('/data/lost_link_extractor.out', 'a+')

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
