# Pythono3 code to rename multiple 
# files in a directory or folder
  
# importing os module
import os
  
# Function to rename multiple files
def main():
    year='2021'
    folder='./swimslog/'

    srcList=['dataApr-14-2021.json','dataApr-14-2021 (1).json', 'dataApr-14-2021 (2).json', 'dataApr-14-2021 (3).json',\
        'dataApr-14-2021 (4).json', 'dataApr-14-2021 (5).json', 'dataApr-14-2021 (6).json', 'dataApr-14-2021 (7).json', \
            'dataApr-14-2021 (8).json', 'dataApr-14-2021 (9).json', 'dataApr-14-2021 (10).json', 'dataApr-14-2021 (11).json']
    
    dstList=['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
  
    for idx, f in enumerate(srcList):
        src =folder + year + '/' + f
        dst =folder + year + '/' + dstList[idx] + '-' + year + ".json"

        # rename() function will rename all the files
        os.rename(src, dst)
  
# Driver Code
if __name__ == '__main__':
      
    # Calling main() function
    main()