#
# <data_visualizer1>
# Author : Kai Bonsol
#

import pandas as pd
import matplotlib.pyplot as plt

if __name__ == "__main__":

    df = pd.read_csv('poll_data/2020-08-04.csv')
    reversed_df = df.iloc[::-1]
    lines = ['approve', 'disapprove']
    reversed_df.plot(x='date', y=lines)
    lines = ['death_count']
    reversed_df.plot(x='date', y=lines)
    plt.show()
    
