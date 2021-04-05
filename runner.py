from database import manager
from crawler.spiders.us_equity import EikonUsEqityHist


manager.init_db()


'''
eikon_us_equity_hist = EikonUsEqityHist()
eikon_us_equity_hist.requests()
'''