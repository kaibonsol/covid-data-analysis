##<visualize_data1.py>
##
## Author: Kai Bonsol
## This program visualizes csv files in a variety of ways, using user input.
##

import os
import os.path
import pandas as pd
import pickle
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib import style

style.use('ggplot')

def getTimeIndices():

    with open('pickles/time_index_data.pickle', 'rb') as f:
        time_index_data = pickle.load(f)

    print(time_index_data)

def visualizeDataByScatterPlot(x1, y1, title, file_name, date, label_points_by='State'):

    df = pd.read_csv('covid_data_by_day/{}.csv'.format(file_name))
    ax = df.plot(x=x1,y=y1, kind='scatter', title=title)
    a = pd.concat({x1 : df[x1], y1 : df[y1], label_points_by : df[label_points_by]}, axis=1)

    for i, point in a.iterrows():
        ax.text(point[x1], point[y1], str(point[label_points_by]))

    fig1 = plt.gcf()
    plt.show()
    fig1.savefig('covid_data_by_day/figures/scatter_plot_{}.png'.format(date))

def userInterface():
    
    # give options for mode of visualization

    print('MODES of visualization:')
    print('[1] COVID cases to COVID tweets')
    print('[2] COVID deaths to COVID tweets')
    print('[3] COVID cases per day to COVID tweets')
    print('[4] COVID death rate to COVID tweets')

    # these two are open to multiple lines
    print('[5] COVID tweets over time for a particular state')
    print('[6] COVID tweets over time for a particular county')
    print('[7] COVID testing to COVID tweets')
    print('[8] COVID cases : recovered to COVID tweets')

    mode = input("Enter a number to select MODE > ")
    file_name = input("Enter a filename > ")
    date = input("Enter figure ID in id_M_D_Y format > ")

    if mode == '1':
        visualizeDataByScatterPlot('Case_Count', 'Num_Tweets',
                'COVID19 Case Count to COVID19 Tweet Count', file_name, date)
    elif mode == '2':
        visualizeDataByScatterPlot('Deaths', 'Num_Tweets',
                'COVID19 Deaths to COVID19 Tweet Count', file_name, date)
    elif mode == '3':
        visualizeDataByScatterPlot('Cases_Per_Day', 'Num_Tweets',
                'COVID19 Cases Per Day to COVID19 Tweet Count', file_name, date)
    #elif mode == '4':
        #visualizeDataByScatterPlot('')
    
if __name__ == "__main__":

    userInterface()
        
