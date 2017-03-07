# -*- coding: utf-8 -*-

from scrapy.spiders import CrawlSpider
from scrapy import Request, FormRequest
from scrapy.utils.request import request_fingerprint
from ResearchGateSpider.items import ResearchGateItem
from ResearchGateSpider.datafilter import DataFilter
from ResearchGateSpider.func import parse_text_by_multi_content
from scrapy.exceptions import CloseSpider
#from scrapy_splash import SplashRequest
#from scrapy_splash import SplashMiddleware
import hashlib
import time


class RGSpider1(CrawlSpider):
    name = 'url_test_link302'
    college_name = 'utulsa'
    college_id = '11'
    country_id = '1'
    state_id = '1'
    city_id = '1'
    allowed_domains = ['utulsa.edu']
    start_urls = ['https://utulsa.edu/']
    
    rules =(

        Rule(LinkExtractor(allow_domains=allowed_domains,
                           allow=('colleges-and-schools','schools_and_colleges','schools-colleges','content',r'[\w\-]+Faculty',r'academics([\-\w\d]+){0,1}','faculty',
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
            lostitem_str = 'The lost url is ' + response.url + '\n'
            self.lostitem_file.write(lostitem_str)
            self.lostitem_file.close()
            raise CloseSpider(reason=u'被封了，准备切换ip')
        item = CandidateBasicItem()
        item['country_id'] = self.country_id
        item['college_id'] = self.college_id
        item['discipline_id'] = '0'

        item['url'] = response.url
        # item['source_code'] = response.body
        item['source_text'] = response.xpath("//text()").extract()
        item['header_title'] = response.xpath('//head/title/text()').extract()

        return item
        pass

    def start_requests11(self):
        headers = {
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36"
        }
        alphabet_list = ["A", "B", "C", "D",
                         "E", "F", "G", "H",
                         "I", "J", "K", "L"]
                         # "M", "N", "O", "P",
                         # "Q", "R", "S", "T",
                         # "U", "V", "W", "X",
                         # "Y", "Z", "Other"]
        for alphabet in alphabet_list:
            url = "https://www.researchgate.net/directory/profiles/"+alphabet
            yield Request(url, headers=headers, callback=self.parse_profile_directory, dont_filter=True)
            #break

        # url = "https://www.researchgate.net/directory/profiles/" + alphabet_list[0]
        # print url
        # yield Request(url, callback=self.parse_profile_directory, dont_filter=True)

    def parse_profile_directory(self, response):
        if response.status == 429:
            lostitem_str = 'first level directory: ' + response.url + '\n'
            self.lostitem_file.write(lostitem_str)
            self.lostitem_file.close()
            raise CloseSpider(reason=u'被封了，准备切换ip')
        headers = response.request.headers
        headers["referer"] = response.url
        for url in response.xpath(
                '//ul[contains(@class, "list-directory")]/descendant::a/@href'). \
                extract():
            url = self.domain + "/" + url
            yield Request(url, headers=headers, callback=self.parse_profile_directory2, dont_filter=True)
            #break

        # urls = response.xpath('//ul[contains(@class, "list-directory")]/descendant::a/@href').extract()
        # url0 = self.domain + "/" + urls[0]
        # print url0
        # yield Request(url0, callback=self.parse_profile_directory2, dont_filter=True)

    def parse_profile_directory2(self, response):
        if response.status == 429:
            lostitem_str = 'second level directory: ' + response.url + '\n'
            self.lostitem_file.write(lostitem_str)
            self.lostitem_file.close()
            raise CloseSpider(reason='被封了，准备切换ip')
        headers = response.request.headers
        headers["referer"] = response.url
        for url in response.xpath(
                '//ul[contains(@class, "list-directory")]/descendant::a/@href'). \
                extract():
            url = self.domain + "/" + url
            yield Request(url, headers=headers, callback=self.parse_profile_directory3, dont_filter=True)
            #break
        # urls = response.xpath('//ul[contains(@class, "list-directory")]/descendant::a/@href').extract()
        # url0 = self.domain + "/" + urls[0]
        # print url0
        # yield Request(url0, callback=self.parse_profile_directory3, dont_filter=True)

    def parse_profile_directory3(self, response):
        if response.status == 429:
            lostitem_str = 'third level directory: ' + response.url + '\n'
            self.lostitem_file.write(lostitem_str)
            self.lostitem_file.close()
            raise CloseSpider(reason='被封了，准备切换ip')
        headers = response.request.headers
        headers["referer"] = response.url
        person_selectors = response.xpath('//ul[contains(@class, "list-directory")]/li')
        for person in person_selectors:
            item = ResearchGateItem()
            item['fullname'] = DataFilter.simple_format(person.xpath('.').extract())
            person_url = self.domain + '/' + DataFilter.simple_format(person.xpath('./a/@href').extract())
            item['link'] = person_url
            item['person_key'] = hashlib.sha256(person_url).hexdigest()
            yield item
        # for url in response.xpath(
        #         '//ul[contains(@class, "list-directory")]/li'). \
        #         extract():
        #     url = self.domain + "/" + url
        #     yield Request(url, headers=headers, callback=self.parse_candidate_overview, dont_filter=True)
             #break
        #urls = response.xpath('//ul[contains(@class, "list-directory")]/descendant::a/@href').extract()
        #url0 = self.domain + "/" + urls[1]
        #print url0
        #yield Request(url0, headers=headers, callback=self.parse_candidate_overview, dont_filter=True)

    def __init__(self, **kwargs):
        super(url_test_link302, self).__init__(**kwargs)
        self.lostitem_file = open('/data/lost_link_extractor.out', 'a+')
        pass

    def close(self, reason):
        self.lostitem_file.close()
        super(url_test_link302, self).close(self, reason)
