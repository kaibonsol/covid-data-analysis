# <transfer_files1.py>
# transfer figure files to chilldudes.com
#

import ftplib

def sendImageToSite(week_name, file_name):

    session = ftplib.FTP('ftpupload.net','epiz_23039397','PPy9oKXDu')
    session.cwd('htdocs/datasets/covid')

    if week_name not in session.nlst():
        session.mkd(week_name)

    session.cwd(week_name)      
    
    file = open('covid_data_by_day/figures/' + file_name, 'rb')
    session.storbinary('STOR ' + file_name, file)
    file.close()
    session.quit()

if __name__ == '__main__':

    sendImageToSite('07_13_2020to07_17_2020','scatter_plot_aga_07_13_2020.png')    
