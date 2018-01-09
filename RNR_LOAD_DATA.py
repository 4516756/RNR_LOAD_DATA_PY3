# coding:utf-8
import urllib
import json
import pyodbc
import urllib.request
import urllib.parse
import datetime
import base64


def setproxy():
    #proxy = urllib.\ProxyHandler({'http': 'CNPriProxy.aia.biz:10938'})
    proxy = urllib.request.ProxyHandler({'http': 'CNPriProxy.aia.biz:10938'})
    opener = urllib.request.build_opener(proxy)
    urllib.request.install_opener(opener)


def getToken():
    setproxy()

    url = 'http://gdaia.iok.la:443/admin/open/activityentOpen/activityentOpenAction!getActivityentToken.action'
    req = urllib.request.Request(url)

    response = urllib.request.urlopen(req)
    s1 = response.read()
    s2 = json.loads(s1)
    token = s2['data']['token']
    print(token)
    return token


def getActList():
    token = getToken()

    endtime = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
    delta = datetime.timedelta(days=-30)
    starttime = datetime.datetime.strftime(datetime.datetime.now() + delta, '%Y-%m-%d')
    print(starttime)
    print(endtime)

    # data = {'startTime':'2017-10-01 00:00','endTime':'2017-10-31 23:59'}
    data = {'startTime': starttime, 'endTime': endtime}
    setParameters = {'corpId': 'wxca69ebedc06fe459', 'data': data, 'token': token}
    # print setParameters
    url = "http://gdaia.iok.la:443/admin/open/activityentOpen/activityentOpenAction!getExportActivityList.action"
    req = urllib.request.Request(url)
    send_data = urllib.parse.urlencode(setParameters)
    send_data = send_data.encode('utf-8')
    response = urllib.request.urlopen(req, send_data)
    s1 = response.read()
    s2 = json.loads(s1)

    sqlserver = pyodbc.connect('DRIVER={SQL Server};SERVER=CFSWKD1400677;DATABASE=GDRNR')
    cursor = sqlserver.cursor()
    SQLSTRING = 'truncate table load_actlist'
    cursor.execute(SQLSTRING)
    cursor.commit()

    datalist = s2['data']['list']
    for r in datalist:
        print(r['createTime'] + "," + r['id'])
        time1 = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
        SQLSTRING = "insert into load_actlist"
        # SQLSTRING = SQLSTRING +" values('"+r['id']+"','"+r['activityType']+"','"+r['startTime']+"','"+r['inNum']+"','"+r['signNum']+"','"+r['title']+"','"+r['addr']+"','"+r['joinNum']+"','"+r['creator']+"','"+r['city']+"','"+r['camp']+"','"+r['family']+"','"+r['director']+"','"+r['status']+"','2017-10-21 15:14:000')"
        SQLSTRING = SQLSTRING + " values('" + r['id'] + "','" + r['activityType'] + "','" + r['startTime'] + "'," + str(
            r['inNum']) + "," + str(r['signNum']) + ",'" + r['title'].replace(',', '!') + "','" + r['addr'].replace(',',
                                                                                                                    '!').replace(
            "'", " ") + "'," + str(r['joinNum']) + ",'" + str(r['creator']) + "','" + (
                    r['city'] if r['city'] is not None else "") + "','" + r['camp'] + "','" + r['family'] + "','" + r[
                        'director'] + "','" + r['status'] + "','" + r['createTime'] + "','" + r[
                        'endTime'] + "','" + time1 + "',NULL,NULL,NULL)"
        cursor.execute(SQLSTRING)
        cursor.commit()


def getPerson():
    token = getToken()

    delta = datetime.timedelta(days=1)
    endtime = datetime.datetime.strftime(datetime.datetime.now() + delta, '%Y-%m-%d')
    delta = datetime.timedelta(days=-4)
    starttime = datetime.datetime.strftime(datetime.datetime.now() + delta, '%Y-%m-%d')
    print(starttime)
    print(endtime)
    # data = {'startTime':starttime,'endTime':endtime}
    data = {'startUpdateTime': starttime, 'endUpdateTime': endtime}
    # data = {'startUpdateTime':'2017-10-30','endUpdateTime':'2017-11-11'}
    setParameters = {'corpId': 'wxca69ebedc06fe459', 'data': data, 'token': token}
    # print setParameters
    url = "http://gdaia.iok.la:443/admin/open/activityentOpen/activityentOpenAction!getExportPreIncreaseList.action"
    req = urllib.request.Request(url)
    send_data = urllib.parse.urlencode(setParameters)
    send_data = send_data.encode('utf-8')
    response = urllib.request.urlopen(req, send_data)
    s1 = response.read()
    s2 = json.loads(s1)

    sqlserver = pyodbc.connect('DRIVER={SQL Server};SERVER=CFSWKD1400677;DATABASE=GDRNR')
    cursor = sqlserver.cursor()
    cursor.execute('truncate table load_person')
    cursor.commit()

    datalist = s2['data']['list']
    for r in datalist:
        time1 = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
        phone = str(base64.b64decode(r['phone']),encoding='utf-8')
        if r['createTime'] != r['updateTime']:
            print(phone, r['createTime'], r['updateTime'])
        print(r['openId'] + ",    " + r['createTime'])

        SQLSTRING = "insert into load_person"
        SQLSTRING = SQLSTRING + " values('" + r['openId'] + "','" + r['personName'][0:30].replace('\'',
                                                                                                  '!') + "','" + phone + "','" + str(
            r['isSale']) + "','" + str(r['code']) + "','" + r['referrerCode'] + "','" + r['referrer'][0:30].replace(
            '\'', '!') + "','" + r['age'] + "','" + r['sex'] + "','" + r['source'] + "','" + r['job'] + "','','" + r[
                        'ccDesc'] + "','" + r['eopStatus'] + "','" + r['focus'] + "','" + r['friendsStatus'] + "','" + \
                    r['sendProjectBook'] + "','" + r['starStatus'] + "','" + r['canEntry'] + "','" + r['city'][
                                                                                                     0:10] + "','" + r[
                        'createTime'] + "','" + r['lastJoinTime'] + "','" + r['updateTime'] + "','" + time1 + "',NULL)"
        cursor.execute(SQLSTRING)
        cursor.commit()

    cursor.execute("EXEC PROCESS_PERSON")
    cursor.commit()


def getActDetial(openid, token):
    # token=getToken()
    # sqlserver = pyodbc.connect('DRIVER={SQL Server};SERVER=CFSWKD1400677;DATABASE=GDRNR')
    # cursor = sqlserver.cursor()
    # cursor.execute('truncate table load_sign')
    # cursor.commit()

    print(openid)
    data = {'id': openid}
    setParameters = {'corpId': 'wxca69ebedc06fe459', 'data': data, 'token': token}
    # print setParameters
    url = "http://gdaia.iok.la:443/admin/open/activityentOpen/activityentOpenAction!getExportActivityDetail.action"
    req = urllib.request.Request(url)
    send_data = urllib.parse.urlencode(setParameters)
    send_data = send_data.encode('utf-8')
    response = urllib.request.urlopen(req, send_data)
    s1 = response.read()
    s2 = json.loads(s1)

    sqlserver = pyodbc.connect('DRIVER={SQL Server};SERVER=CFSWKD1400677;DATABASE=GDRNR')
    cursor = sqlserver.cursor()

    datalist = s2['data']['list']
    for r in datalist:
        time1 = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
        SQLSTRING = "insert into load_sign"
        SQLSTRING = SQLSTRING + " values('" + openid + "','" + r['openId'] + "','" + r['personName'][0:30].replace('\'',
                                                                                                                   '') + "','" + \
                    r['createTime'] + "','" + r['referrerCode'] + "','" + r['signTime'] + "','" + r[
                        'activityType'] + "','" + r['addr'][0:100] + "','" + r['currentlen'] + "')"
        cursor.execute(SQLSTRING)
        cursor.commit()

    # cursor.execute('exec process_actlist')
    # cursor.commit()


def loadActDetails():
    sqlserver = pyodbc.connect('DRIVER={SQL Server};SERVER=CFSWKD1400677;DATABASE=GDRNR')
    cursor = sqlserver.cursor()
    cursor.execute('truncate table load_sign')
    cursor.commit()

    # cursor.execute("""select A.* from load_actlist A
    #                  where inNum>0
    #               """)

    cursor.execute("""select A.* from load_actlist A 
LEFT OUTER JOIN [dbo].[actlist] B ON A.id=B.ID 
WHERE b.filestatus is null or (b.preagtroll is null or b.preagtroll=0)""")

    token = getToken()

    for r in cursor:
        getActDetial(r.id, token)

    cursor.execute('exec process_actlist')
    cursor.commit()


def main():
    getPerson()
    getActList()
    loadActDetails()

    # token=getToken()
    # getActDetial('2a7b4bbe-43d9-4052-ac57-f397f3921c6a',token)

    print("----------completed---------------")


if __name__ == '__main__':
    main()