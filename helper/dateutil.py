from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import calendar
import logging

def get_iso_date(frequency, date):
    date_len = len(date)
    if date_len == 8:
        d = datetime.strptime(date,'%Y%m%d')
    elif date_len == 6:
        d = datetime.strptime(date,'%Y%m')
    else:
        d = datetime.strptime(date,'%Y')
    if frequency == 'W':
        # 해당 날짜가 속한 iso week 기록
        #datetime.strptime(s+'-5','%G-W%V-%u')
        iso_cal = d.isocalendar()
        return f'{iso_cal[0]}-W{str(iso_cal[1]).zfill(2)}'
    elif frequency == 'M':
        return d.strftime('%Y-%m')
    elif frequency == 'Q':
        q = pd.to_datetime(d).to_period('Q')
        return f'{q.year}-Q{q.quarter}'
    elif frequency == 'Y':
        return d.strftime('%Y')
    else:
        # including frequency null and D
        return d.strftime('%Y-%m-%d')

def get_adj_date(iso_date):
    date_len = len(iso_date)
    if 'Q' in iso_date:
        return pd.Period(iso_date).strftime('%Y%m%d')
    elif 'W' in iso_date:
        return datetime.strptime(iso_date+'-5','%G-W%V-%u').strftime('%Y%m%d')
    elif date_len == 10:
        return datetime.strptime(iso_date,'%Y-%m-%d').strftime('%Y%m%d')
    elif date_len == 7:
        d = datetime.strptime(iso_date,'%Y-%m')
        return f"{d.strftime('%Y%m')}{calendar.monthrange(d.year, d.month)[1]}"
    elif date_len == 4:
        return iso_date+'1231'
    else:
        raise ValueError('Not isodate')

def get_datetime_offset(d, offset, mode):
    if mode == 'year':
        d += relativedelta(years=offset)
    elif mode == 'month':
        d += relativedelta(months=offset)
    elif mode == 'week':
        d += timedelta(weeks=offset)
    elif mode == 'day':
        d += timedelta(days=offset)
    return d

def calculate_iso_date(iso_date, offset=-1, mode=None):
    date_len = len(iso_date)
    if 'Q' in iso_date:
        if mode == 'year':
            q = pd.Period(iso_date) + (offset*4)
        else:
            q = pd.Period(iso_date) + offset
        return f'{q.year}-Q{q.quarter}'
    elif 'W' in iso_date:
        if mode is None:
            mode = 'week'
        d = get_datetime_offset(datetime.strptime(iso_date+'-5','%G-W%V-%u'), offset, mode)
        iso_cal = d.isocalendar()
        return f'{iso_cal[0]}-W{str(iso_cal[1]).zfill(2)}'
    elif date_len == 10:
        if mode is None:
            mode = 'day'
        d = get_datetime_offset(datetime.strptime(iso_date,'%Y-%m-%d'), offset, mode)
        return d.strftime('%Y-%m-%d')
    elif date_len == 7:
        if mode is None:
            mode = 'month'
        d = get_datetime_offset(datetime.strptime(iso_date,'%Y-%m'), offset, mode)
        return d.strftime('%Y-%m')
    elif date_len == 4:
        return str(int(iso_date) + offset)
    else:
        raise ValueError('Not isodate')

class DateParser:
    """업데이트 가능한 Date 컬렉터로서
    지정된 format과 frequency정보에 따라 표준포맷(frequency에 따라 y:%Y, m:%Y%m, d:%Y%m%d)으로 값을 리턴하며,
    업데이트 될때마다 최종 날짜와 최초날짜를 기록한다.
    """

    def __init__(self, format, frequency=""):
        self.format = format
        self.frequency = frequency
        self.last_date = "0"
        self.first_date = "99999999"

    def _update_last_date(self):
        if self.last_date < self.current_date:
            self.last_date = self.current_date
        if self.first_date > self.current_date:
            self.first_date = self.current_date

    def _parse_date(self, datestr, type):

        self.format = "%Y%m%d"

        if len(datestr) <= 4 :
            self.frequency = "y"
            self.format = "%Y"
        elif len(datestr) <= 6 :
            if type == "q1": #1,2,3,4로 처리되는 경우 
                year = datestr[:4]
                qt = int(datestr[5:]) * 3
                datestr = year + str(qt).zfill(2)

            self.frequency = "m"
            self.format = "%Y%m"                
       
        else  :
            self.frequency = "d"        
            self.format = "%Y%m%d"                        

        date_obj = datetime.strptime(datestr, self.format)
        
        if self.frequency == "y" :
            self.current_date = date_obj.strftime("%Y") + "1231"
        elif self.frequency == "m" :
            day = calendar.monthrange(date_obj.year, date_obj.month)[1]
            self.current_date = date_obj.strftime("%Y%m") + str(day)
        else:
            self.current_date = date_obj.strftime("%Y%m%d")

        self._update_last_date()

    def update(self, datestr,type=""):
        self._parse_date(datestr, type)

    def get(self):
        return self.current_date

    def get_year(self):
        if self.current_date and len(self.current_date) >= 4:
            return self.current_date[:4]

    def get_month(self):
        if self.current_date and len(self.current_date) >= 6:
            return self.current_date[4:6]

    def get_day(self):
        if self.current_date and len(self.current_date) >= 8:
            return self.current_date[6:]

import uuid
class LastDateCollector:
    """최종날짜만 기록하는 컬렉터
    """

    _last_date = '00000000'

    def force_set_date(self, date):
        self._last_date = date

    def update(self, current_date):
        if self._last_date < current_date:
            self._last_date = current_date

    def get(self):
        return self._last_date