# coding=utf-8
# 获取网站的www 信息

from bs4 import BeautifulSoup
import urllib2


class wwwInfo:
    '''
    抓取网站的标题等信息
    '''

    @classmethod
    def start_parse(self, domain, contacttool_info, getContactFlag):
        # socket.setdefaulttimeout(5)
        # url = 'http://www.' + domain
        # content = ''
        # try:
        #     content = urllib.urlopen(url)
        #     if not content:
        #         url = 'http://' + domain
        #         content = urllib.urlopen(url)
        # except Exception as ex:
        #     return {}
        # if not content:
        #     return {}

        url = 'http://' + domain
        content = ''
        try:
            content = urllib2.urlopen(url, timeout=10)
            if not content:
                url = 'http://www.' + domain
                content = urllib2.urlopen(url, timeout=10)
        except Exception as ex:
            return {}
        if not content:
            return {}
        brand_info = {}
        html = ''
        try:
            html = content.read()
        except Exception as e:
            return {'title': '', 'contacttool': brand_info}
        title = wwwInfo.get_meta_info(html)
        if getContactFlag and content:
            try:
                brand_info = wwwInfo.get_contacttool_info(html, contacttool_info)
            except Exception as e:
                pass
        return {'title': title, 'contacttool': brand_info}

    @classmethod
    def get_meta_info(self, content):
        try:
            soup = BeautifulSoup(content, 'lxml')
            # 获取网站标题
            title = soup.find('title')
            if title:
                title = title.string
            else:
                title = soup.find(attrs={"name": "title"})
                if title:
                    title = title['content']
                    # # 获取网站的关键词
                    # keywords = soup.find(attrs={"name": "keywords"})
                    # if keywords:
                    #     keywords = keywords['content']
                    # else:
                    #     keywords = soup.find(attrs={"name": "KEYWORDS"})
                    #     if keywords:
                    #         keywords = keywords['content']
                    #
                    # description = soup.find(attrs={"name": "description"})
                    # if description:
                    #     description = description['content']
                    # else:
                    #     description = soup.find(attrs={"name": "DESCRIPTION"})
                    #     if description:
                    #         description = description['content']

                    # return {'title': title, 'keywords': keywords, 'description': description}
            return title
        except Exception as ex:
            return ''

    @classmethod
    def get_contacttool_info(self, content, contacttool_info):
        for (domain, info) in contacttool_info.items():
            if domain in content:
                return info
        return {}

# # 测试数据
# contacttool_info = {
#     'qiyukf.com': {'brand_id': 1, 'brand_name': '七鱼智能客服'},
# }
# #
# # # print wwwInfo.start_parse('qiangbi.net', contacttool_info, True)
# print wwwInfo.start_parse('qiyukf.com', contacttool_info, True)
