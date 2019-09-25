# coding=utf-8
# 获取网站的www 信息
import urllib.request

import sys
from builtins import print

from bs4 import BeautifulSoup

type = sys.getfilesystemencoding()


class wwwInfo:
    '''
    抓取网站的标题等信息
    '''

    @classmethod
    def start_parse(self, domain, contacttool_info, getContactFlag):
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
        title = wwwInfo.get_meta_info(content)
        if getContactFlag and content:
            try:
                brand_info = wwwInfo.get_contacttool_info(content, contacttool_info)
            except Exception as e:
                pass
        return {'title': title, 'contacttool': brand_info}

    @classmethod
    def get_meta_info(self, content):
        try:
            soup = BeautifulSoup(content, 'html.parser')
            # 获取网站标题
            title = soup.title.string
            if title:
                return title
            return ''
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
            content = urllib.request.urlopen(url, timeout=10)
            if not content:
                url = 'http://www.' + domain
                content = urllib.request.urlopen(url, timeout=10)
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
