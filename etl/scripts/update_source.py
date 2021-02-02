# -*- coding: utf-8 -*-

from ddf_utils.factory.common import download


files = [
    dict(filename='../source/TB_data_dictionary.csv',
         url='https://extranet.who.int/tme/generateCSV.asp?ds=dictionary'),
    dict(filename='../source/TB_burden_countries.csv',
         url='https://extranet.who.int/tme/generateCSV.asp?ds=estimates'),
    dict(filename='../source/MDR_RR_TB_burden_estimates.csv',
         url='https://extranet.who.int/tme/generateCSV.asp?ds=mdr_rr_estimates'),
    dict(filename='../source/LTBI_estimates.csv',
         url='https://extranet.who.int/tme/generateCSV.asp?ds=ltbi_estimates'),
    dict(filename='../source/TB_burden_age_sex.csv',
         url='https://extranet.who.int/tme/generateCSV.asp?ds=estimates_age_sex'),
    dict(filename='../source/TB_notifications.csv',
         url='https://extranet.who.int/tme/generateCSV.asp?ds=notifications')
]


def main():
    for f in files:
        download(f['url'], f['filename'], resume=False)

    print('done.')


if __name__ == '__main__':
    main()
