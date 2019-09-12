# coding=utf-8
# 获取网站的www 信息
import urllib.request

from bs4 import BeautifulSoup


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
            content = urllib.request.urlopen(url, timeout=10)
            if not content:
                url = 'http://www.' + domain
                content = urllib.request.urlopen(url, timeout=10)
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

    '''
    解析下 mail.域名 的信息
    '''

    @classmethod
    def startParseMailIndex(self, domain, mailSelfBuildInfo):
        url = 'http://mail.' + domain
        content = ''
        try:
            content = urllib.reequest.urlopen(url, timeout=10)
            if not content:
                url = 'http://www.' + domain
                content = urllib.reequest.urlopen(url, timeout=10)
        except Exception as ex:
            return {}
        if not content:
            return {}
        brandInfo = {}
        html = ''
        try:
            html = content.read()
        except Exception as e:
            return {}
        title = wwwInfo.get_meta_info(html)
        try:
            brandInfo = wwwInfo.getSelfBuildInfo(html, mailSelfBuildInfo)
        except Exception as e:
            pass
        return {'title': title, 'brandInfo': brandInfo}

    '''
    获取自建的相关信息
    '''

    @classmethod
    def getSelfBuildInfo(self, content, mailSelfBuildInfo):
        for (domain, info) in mailSelfBuildInfo.items():
            if domain in content:
                return info
        return {}


# 域名自建相关数据
mailSelfBuildInfo = {
    'coremail': {'brand_id': 1, 'brand_name': '盈世'},
    'fangmail': {'brand_id': 2, 'brand_name': '方向标'},
    'winmail': {'brand_id': 3, 'brand_name': 'winmail'},
    'anymacro': {'brand_id': 4, 'brand_name': '安宁'},
    'turbomail': {'brand_id': 5, 'brand_name': 'TurboMail'},
    'u-mail': {'brand_id': 6, 'brand_name': 'U-Mail'},
    'exchange': {'brand_id': 7, 'brand_name': 'Exchange'},
    'microsoftonline': {'brand_id': 8, 'brand_name': '微软Office365'},
}

# print wwwInfo.startParseMailIndex('jxfhgarment.com', mailSelfBuildInfo)
