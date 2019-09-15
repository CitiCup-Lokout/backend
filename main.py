from flask import Flask
from flask_restful import reqparse, abort, Api, Resource
from flask_cors import CORS

import os
import time
import datetime
import numpy as np
import pandas as pd
import requests
import json

# Host SSH
# root@49.232.24.23
# Zmty!233$

def parse_datetime_in_filename(row, column):
    dt = time.mktime(time.strptime('2019-'+row[column].strip(), '%Y-%m-%d %H.csv'))
    return dt


def parse_datetime(row, column, fmt='%Y-%m-%d %H:%M:%S'):
    dt = row[column]
    if isinstance(row[column], str):
        dt = time.mktime(time.strptime(row[column].strip(), fmt))
    return dt

def parse_int(row, column):
    return int(row[column])

def parse_float(row, column):
    return float(row[column])

def fixMergedVideoData(json_buf):
    replacements = (
        ('Aid', 'AVNum'), ('Name', 'Topic'), ('Time', 'UploadTime')
        , ('Danmaku', 'DMNum'), ('DMnum', 'DMNum'), ('reply', 'Comment')
        , ('favorite', 'Save'), ('coin', 'Coin'), ('like', 'Like')
    )
    for origin, to in replacements:
        json_buf = json_buf.replace('"%s":' % origin, '"%s":' % to)
    return json_buf


def read_historical_json(path):
    with open(path, 'r', encoding="utf-8") as f:
        json_buf = f.read()
        json_buf = fixMergedVideoData(json_buf)
        lines = [line.strip(' \n\r\t[],') for line in json_buf.split('\n')]
        json_buf = ','.join([line for line in lines if len(line) != 0])
        json_buf = json_buf.strip(' \t\n\r,')
        json_buf = '[' + json_buf + ']'
        # with open('test.json', 'w') as o:
        #     o.write(json_buf)
        df = pd.read_json(json_buf, orient='records', encoding='utf-8')

        casts = (
            ('AVNum', parse_int), ('UploadTime', parse_datetime)
            , ('DMNum', parse_float), ('Comment', parse_float)
            , ('Save', parse_float), ('Coin', parse_float)
            , ('Like', parse_float)
        )
        for col, parser in casts:
            df[col] = df.apply(parser, axis=1, args=(col,))
        df.drop_duplicates(subset='AVNum', inplace=True)
        return df

def read_tracked_video_json(path):
    with open(path, 'r', encoding="utf-8") as f:
        json_buf = f.read()
        lines = [line.strip(' \n\r\t[],') for line in json_buf.split('\n')]
        json_buf = ','.join([line for line in lines if len(line) != 0])
        json_buf = json_buf.strip(' \t\n\r,')
        json_buf = '[' + json_buf + ']'
        # with open('test.json', 'w') as o:
        #     o.write(json_buf)
        df = pd.read_json(json_buf, orient='records', encoding='utf-8')

        casts = (
            ('CrawlTime', parse_datetime_in_filename)
            , ('UploadTime', parse_datetime)
        )
        for col, parser in casts:
            df[col] = df.apply(parser, axis=1, args=(col,))
        return df

class UpProfile(Resource):
    def get(self, uid):
        df = pd.read_json('../highlevel/a.json', orient='records', encoding='utf-8')
        df = df.loc[df['uid'] == uid]
        if df.shape[0] == 0:
            abort(404, message="Up %d doesn't exist".format(uid))
        elif df.shape[0] >= 2:
            abort(400, message="Uid %d is not unique.".format(uid))
        df.fillna('NaN', inplace=True)
        return df.to_dict('records')[0]

        '''
        #######################################################################

        df = pd.read_json('../Apic/a.json', orient='records', encoding='utf-8')
        df = df.loc[df['uid'] == uid]
        if df.shape[0] == 0:
            abort(404, message="Up %d doesn't exist".format(uid))
        elif df.shape[0] >= 2:
            abort(400, message="Uid %d is not unique.".format(uid))
        df.drop('Time', axis=1, inplace=True)
        info = df.to_dict('records')[0]

        #######################################################################

        csv_path = os.path.join('../A', str(uid)+'.csv')
        if not os.path.exists(csv_path):
            abort(404, message="Up %d doesn't exist".format(uid))
        df = pd.read_csv(csv_path)
        df = df.apply(parse_datetime, axis=1, args=('Time',))
        df.sort_values('Time', axis=0, inplace=True, ascending=True)

        today = datetime.date.today()
        first_day = datetime.date(year=today.year, month=today.month, day=1)
        timestamp = time.mktime(first_day.timetuple())
        this_month = df.loc[df['Time'] > timestamp, :]
        if not this_month.empty:
            first_day_row = this_month.iloc[0, :]
            this_day_row = this_month.iloc[-1, :]

            info['ViewsFirstDayInMonth'] = first_day_row['PlayNum']
            info['ViewsMonthly'] = this_day_row['PlayNum'] - first_day_row['PlayNum']
            info['ChargesMonthly'] = this_day_row['ChargeNum']
            info['ChargeNum'] = info['ChargesMonthly']

        today = datetime.date.today()
        week_ago = today - datetime.timedelta(days=7)
        timestamp = time.mktime(week_ago.timetuple())
        this_week = df.loc[df['Time'] > timestamp, :]
        if not this_week.empty:
            week_ago_row = this_week.iloc[0, :]
            this_day_row = this_week.iloc[-1, :]

            info['ViewsWeekAgo'] = week_ago_row['PlayNum']
            info['ViewsNow'] = this_day_row['PlayNum']
            info['PlayNum'] = info['ViewsNow']

            info['FansWeekAgo'] = week_ago_row['FanNum']
            info['FansNow'] = this_day_row['FanNum']
            info['FanNum'] = info['FansNow']

        #######################################################################

        json_path = os.path.join('../HistoricalRecords', str(uid)+'.json')
        if not os.path.exists(json_path):
            abort(404, message="Up %d doesn't exist".format(uid))
        df = read_historical_json(json_path)
        df.sort_values('UploadTime', axis=0, inplace=True, ascending=True)

        today = datetime.date.today()
        month_ago = today - datetime.timedelta(days=30)
        timestamp = time.mktime(month_ago.timetuple())
        info['RecentSince'] = timestamp
        info['RecentCount'] = df.loc[df['UploadTime'] >= timestamp, :].shape[0]

        # Last 10 videos
        df = df.tail(n=10)
        df['Score'] = df.apply(lambda v: v['Like'] + 3*v['Coin'] + 5*v['Save'], axis=1)
        info['AvgView'] = df['View'].mean()
        info['AvgScore'] = df['Score'].mean()
        info['AvgQuality'] = info['AvgView'] + info['AvgScore']
        info['AvgDuration'] = df['Duration'].mean()
        info['TotalCount'] = df.shape[0]

        first_day = datetime.datetime.fromtimestamp(df['UploadTime'].min())
        now = datetime.datetime.now()
        days_escaped = (now-first_day).days
        info['Frequency'] = days_escaped / info['TotalCount']

        #######################################################################

        L4, L3, O4, N4, P4 = info['ViewsNow'], info['ViewsWeekAgo'], info['AvgView'], info['AvgQuality'], info['RecentCount']
        info['WorkIndex'] = ((N4**1.1 + (L4-L3)/10) * (N4-O4) / O4 * P4 / 30) ** 0.65 / 10

        K3, K4 = info['FansWeekAgo'], info['FansNow']
        info['FanIncPercentage'] = (K4 - K3) / K3
        info['FanIncIndex'] = ((K4 - K3) * info['FanIncPercentage']) ** 0.75 * (1 if K4 > K3 else -1)

        S3, R4, K4, L4, L3 = info['WorkIndex'], info['FanIncIndex'], info['FansNow'], info['ViewsNow'], info['ViewsWeekAgo']
        info['SummaryIndex'] = (S3+R4)*(K4/1000+(L4-L3)/10000) ** 0.7 / 1000

        info['IncomeYearly'] = info['AvgView']*0.003*12*info['RecentCount'] / 1000 + info['SummaryIndex']*450/1500
        info['IncomePerVideo'] = info['IncomeYearly'] / (info['RecentCount']*12)

        X = info['IncomeYearly']
        info['ChannelValue'] = X + X/(1.1**1) + X/(1.1**2) + X/(1.1**3) + X/(1.1**4)

        #######################################################################

        try:
            url = 'https://api.bilibili.com/x/space/acc/info?mid=%s' % str(uid)
            info['Face'] = requests.get(url).json()['data']['face']
        except:
            info['Face'] = 'Not Found'

        return info
        '''

class Videos(Resource):
    def get(self, uid):
        json_path = os.path.join('../HistoricalRecords', str(uid) + '.json')
        df = read_historical_json(json_path)
        # df = df.loc[:, ['AVNum', 'Author', 'Topic', 'UploadTime', 'Type']]
        df.fillna('NaN', inplace=True)
        return df.to_dict('records')

class VideoInfo(Resource):
    def get(self, uid, aid):
        json_path = os.path.join('../HistoricalRecords', str(uid) + '.json')
        df = read_historical_json(json_path)
        df = df.loc[df['AVNum'] == aid]
        if df.shape[0] == 0:
            abort(404, message="Video %d doesn't exist".format(uid))
        elif df.shape[0] >= 2:
            abort(400, message="Video ID %d is not unique.".format(uid))
        df.fillna('NaN', inplace=True)

        return df.to_dict('records')[0]

class VideoQuality(Resource):
    def get(self, uid):
        json_path = os.path.join('../HistoricalRecords', str(uid)+'.json')
        if not os.path.exists(json_path):
            abort(404, message="Up %d doesn't exist".format(uid))
        df = read_historical_json(json_path)
        df.sort_values('UploadTime', axis=0, inplace=True, ascending=True)
        df = df.tail(n=30)
        def calculateQuality(v):
            if np.isnan(v['View']):
                return v['Like'] + 3*v['Coin'] + 5*v['Save']
            else:
                return v['View'] + v['Like'] + 3*v['Coin'] + 5*v['Save']
        df['Quality'] = df.apply(calculateQuality, axis=1)
        df = df.loc[:, ['AVNum', 'Author', 'Topic', 'UploadTime', 'Type', 'Quality']]

        df.fillna('NaN', inplace=True)

        return df.to_dict('records')

class TrackedVideos(Resource):
    def get(self, uid):
        json_path = os.path.join('../upVideo', str(uid) + '.json')
        if not os.path.exists(json_path):
            abort(404, message="Up %d doesn't exist".format(uid))

        df = read_tracked_video_json(json_path)
        df.sort_values('CrawlTime', axis=0, inplace=True, ascending=False)
        # df = df.loc[:, ['AVNum', 'Author', 'Topic', 'UploadTime', 'Type']]
        df.drop_duplicates(subset='AVNum', keep='first', inplace=True)
        df.fillna('NaN', inplace=True)
        return df.to_dict('records')

class Chart(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('field', type=str)
        self.parser.add_argument('dataType', type=str)

        super(Chart, self).__init__()

    def get(self, uid):
        args = self.parser.parse_args()
        field, dataType = args['field'], args['dataType']
        if not os.path.exists(os.path.join('../A', str(uid)+'.csv')):
            abort(404, message="Up %d doesn't exist".format(uid))

        df = pd.read_csv(os.path.join('../A', str(uid)+'.csv'))
        df = df.loc[:, ['uid', 'Time', field]]
        df['Time'] = df.apply(parse_datetime, axis=1, args=('Time',))
        df.sort_values('Time', axis=0, inplace=True, ascending=True)

        if dataType == 'raw' or dataType == 'sum':
            if field.lower() == 'playnum':
                diff = df[field].diff()
                df = df.loc[diff.abs() > 1e-8]
            # elif field.lower() == 'chargenum':
            #     diff = df[field].diff()
            #     df.loc[diff < 0.0, field] = float('Nan')
        elif dataType == 'inc':
            df[field] = df[field].diff()
            if field.lower() == 'playnum':
                df = df.loc[df[field].abs() > 1e-8]
            elif field.lower() == 'chargenum':
                df.loc[df[field] < 0.0, field] = float('Nan')
        elif dataType == 'pre':
            if field.lower() == 'playnum':
                diff = df[field].diff()
                df = df.loc[diff.abs() > 1e-8]

            if not os.path.exists(os.path.join('../P', str(uid)+'.json')):
                abort(404, message="Up %d prediction data doesn't exist".format(uid))
            pre_df = pd.read_json(os.path.join('../P', str(uid)+'.json'))
            pre_df['uid'] = uid
            pre_df = pre_df.loc[:, ['uid', 'Time', field]]
            if field.lower() != 'channelvalue':
                df = pd.concat((df, pre_df), axis=0, sort=False)
                # df.sort_values('Time', axis=0, inplace=True, ascending=True)
            else:
                df = pre_df

        df.fillna('NaN', inplace=True)
        data_points = df.to_dict('records')

        json_path = os.path.join('../HistoricalRecords', str(uid) + '.json')
        df = read_historical_json(json_path)
        df = df.loc[:, ['AVNum', 'Author', 'Topic', 'UploadTime', 'Type']]
        videos = df.to_dict('records')

        i = j = 0
        while i < len(videos) and j < len(data_points):
            av = videos[i]
            pt = data_points[j]

            if j >= 1:
                prev_pt = data_points[j-1]
                d1 = av['UploadTime'] - prev_pt['Time']
                d2 = av['UploadTime'] - pt['Time']
                if d1 * d2 < 0:
                    d1, d2 = abs(d1), abs(d2)
                    k = j if d1 > d2 else j - 1
                    if 'Videos' not in data_points[k]:
                        data_points[k]['Videos'] = [av]
                    else:
                        data_points[k]['Videos'].append(av)

            if av['UploadTime'] < pt['Time']: i += 1
            else: j += 1

        return data_points

class VideoChart(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('field', type=str)
        self.parser.add_argument('dataType', type=str)

        super(VideoChart, self).__init__()
        
    def get(self, uid, aid):
        args = self.parser.parse_args()
        field, dataType = args['field'], args['dataType']

        df = read_tracked_video_json(os.path.join('../upVideo', str(uid)+'.json'))
        df = df.loc[df['AVNum'] == aid, ['AVNum', 'Author', 'CrawlTime', field]]

        if df.shape[0] == 0:
            abort(404, message="Video %d/%d doesn't have tracked data".format(uid, aid))

        df.sort_values('CrawlTime', axis=0, inplace=True, ascending=True)

        if dataType == 'raw' or dataType == 'sum':
            pass
        elif dataType == 'inc':
            df[field] = df[field].diff()

            subdf = subdf.dropna(subset=[field])

        df.dropna(subset=[field], inplace=True)
        # df.fillna('NaN', inplace=True)

        return df.to_dict('records')

class Search(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('query', type=str)

        super(Search, self).__init__()

    def get(self):
        df = pd.read_json('../highlevel/a.json', orient='records', encoding='utf-8')

        args = self.parser.parse_args()

        def query_score(row, keyword):
            keyword = keyword.lower()
            name = row['Name'].lower()
            score = name.count(keyword) * len(keyword) / len(name)
            return score

        df['query_score'] = df.apply(query_score, axis=1, args=(args['query'],))
        df = df.loc[df['query_score'] > 1e-8]
        df.sort_values(by=['query_score', 'FanNum', 'Name', 'uid'], axis=0, inplace=True, ascending=False)
        df.drop_duplicates(subset='uid', inplace=True)
        df.fillna('NaN', inplace=True)

        return df.to_dict('records')

class Rank(Resource):
    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('field', type=str)
        self.parser.add_argument('offset', type=int)
        self.parser.add_argument('count', type=int)
        self.parser.add_argument('order', type=str)

        super(Rank, self).__init__()

    def get(self, category):
        args = self.parser.parse_args()
        field = args['field']
        offset = args['offset']
        count = args['count']
        order = args['order']

        asc = True if order == 'asc' else False

        info_df = pd.read_json('../highlevel/a.json', orient='records', encoding='utf-8')
        if category.lower() != 'global':
            category_path = os.path.join('../UpinfoClassify/Videotypeclassify', category+'.csv')
            cate_df = pd.read_csv(category_path, names=['uid', '__Name']).loc[:, 'uid']
            df = pd.merge(info_df, cate_df, on='uid')
        else:
            df = info_df

        df.sort_values(by=field, axis=0, inplace=True, ascending=asc)
        df.drop_duplicates(subset='uid', inplace=True)
        df.fillna('NaN', inplace=True)

        if df.shape[0] == 0:
            abort(404, message="No up in %s category".format(category))

        if offset is None or offset < 0 or offset >= df.shape[0]: offset = 0
        df = df.iloc[offset:]

        if count is not None:
            df = df.head(n=count)

        items = df.to_dict('records')
        for i, item in enumerate(items):
            item['Rank'] = i+1+offset
        return items

app = Flask(__name__)
CORS(app)

api = Api(app)
##
## Actually setup the Api resource routing here
##
api.add_resource(Search, '/search')
api.add_resource(UpProfile, '/info/<int:uid>')
api.add_resource(Videos, '/videos/<int:uid>')
api.add_resource(VideoQuality, '/videoQuality/<int:uid>')
api.add_resource(TrackedVideos, '/trackedVideos/<int:uid>')
api.add_resource(VideoInfo, '/videoInfo/<int:uid>/<int:aid>')
api.add_resource(VideoChart, '/videoChart/<int:uid>/<int:aid>')
api.add_resource(Chart, '/chart/<int:uid>')
api.add_resource(Rank, '/rank/<string:category>')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
