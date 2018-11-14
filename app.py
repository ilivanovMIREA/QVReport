try:
    import pyodbc
    from connection import Connect
    import logging
    import schedule
    import time
    import os,sys
    import string,types
    reload(sys)
    sys.setdefaultencoding('utf8')
    import csv
    from all_query import arrSQL
    import subprocess

except ImportError, e:
    raise e

allReport = [
    "C:\QVData\DATA\CIEL_Report.qvw"
    ]

TEST = []

def RunQuery(sql, name):
    data = Connect('CIEL')   
    data.Execute(sql,'fetchall',True, name)
    print("Good QUERY")
    return

def updateQV():
    for report in allReport:
        subprocess.call(["C:\Program Files\QlikView\Qv.exe", "/r", "%s" % report])
    
def copyQV():
    src="C:\Users\IGIVANOV\Documents\Python Scripts\QVReport\Data\CMR_blk_invoice.qvw"
    dst = "M:\QVReport\Accounts\CMR_blk_invoice.qvw"
    cmd='copy "%s" "%s"' % (src, dst)
    status = subprocess.call(cmd, shell=True)

def main(dictionary):
	for i in dictionary:
		RunQuery(arrSQL[i],i)


def job():
    main(arrSQL)
    updateQV()
    copyQV()

schedule.every().day.at("7:30").do(job)

if __name__ == "__main__":
    try:
        while(True):
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print('Finishing Work')