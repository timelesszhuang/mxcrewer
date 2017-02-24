# coding=utf-8
# 获取并且比对 mx 信息 然后更新 更新到制定的数据表中 定期推送

import os


class MxManage:
    '''
    解析ｍｘ数据
    '''

    @classmethod
    def startParseMx(self, domain_name):
        command = 'dig -t mx +short ' + domain_name
        r = os.popen(command)  # 执行该命令
        mxInfo = list()
        priority = 0
        for line in r.readlines():  # 依次读取每行
            line = line.strip()  # 去掉每行头尾空白
            # 包含超时的时候
            if ';; connection timed out;' in line:
                # print('获取mx 超时')
                return {}
            mxInfo.append(line)
            perMx = line.split(' ')
            # print(perMx)
            if len(perMx) == 2:
                # print(perMx[0])
                # if int(perMx[0]) < priority and priority != 0:
                #     # 取最大的mx 记录来标志是用的哪家的mx 方便比较
                #     priority = int(perMx[0])
                # elif priority == 0:
                #     priority= int(perMx[0])
                if priority == 0:
                    priority = int(perMx[0])
                elif priority > int(perMx[0]):
                    priority = int(perMx[0])
                mx = perMx[1]
            else:
                mx = perMx[0]
        # print mxInfo
        if len(mxInfo) == 0:
            # 没有获取到
            # print("没有获取到mx数据")
            return {}
        # MxManage.subMxSuffix(mx)
        return {'priority': priority, 'mx': mx, 'mxsuffix': MxManage.subMxSuffix(mx)}

    '''
    截取mx 后缀信息
    '''

    @classmethod
    def subMxSuffix(self, mx):
        mx = mx[0:len(mx) - 1]
        mxlist = mx.split('.')
        split = 2
        if '.net.cn' in mx:
            split = 3
        elif '.com.cn' in mx:
            split = 3
        elif '.gov.cn' in mx:
            split = 3
        elif '.org.cn' in mx:
            split = 3
        elif 'ym.163.com' in mx:
            split = 3
        elif 'qiye.163.com' in mx:
            split = 3
        suffix = ''
        for i in range(split):
            if suffix:
                suffix = mxlist[len(mxlist) - i - 1] + '.' + suffix
            else:
                suffix = mxlist[len(mxlist) - i - 1]
        return suffix

# MxManage.startParseMx('chinahongguang.com')
