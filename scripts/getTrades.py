import json
from datetime import datetime
import pandas as pd
import io
import requests
# import datetime
import glob

arkk_url = "https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_INNOVATION_ETF_ARKK_HOLDINGS.csv"
arkq_url = "https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_AUTONOMOUS_TECHNOLOGY_&_ROBOTICS_ETF_ARKQ_HOLDINGS.csv"
arkw_url = "https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_NEXT_GENERATION_INTERNET_ETF_ARKW_HOLDINGS.csv"
arkg_url = "https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_GENOMIC_REVOLUTION_MULTISECTOR_ETF_ARKG_HOLDINGS.csv"
arkf_url = "https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_FINTECH_INNOVATION_ETF_ARKF_HOLDINGS.csv"
print3d_url = "https://ark-funds.com/wp-content/fundsiteliterature/csv/THE_3D_PRINTING_ETF_PRNT_HOLDINGS.csv"
israel_url = "https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_ISRAEL_INNOVATIVE_TECHNOLOGY_ETF_IZRL_HOLDINGS.csv"

urls = [arkk_url, arkq_url, arkw_url, arkg_url, arkf_url,print3d_url,israel_url]
ark_etf = [{'name':'ARKK', 'url':arkk_url},{'name':'ARKQ', 'url':arkq_url},{'name':'ARKW','url':arkw_url},
           {'name':'ARKG','url':arkg_url},{'name':'ARKF','url':arkf_url},{'name':'PRNT','url':print3d_url},
          {'name':'IZRL','url':israel_url}]

def getHolding():
    df = pd.DataFrame([], columns=['date','fund','company','ticker','cusip','shares','market value($)','weight(%)'])
    for etf in ark_etf:
        s = requests.get(etf['url']).content
        df_temp = pd.read_csv(io.StringIO(s.decode('utf-8'))).dropna()
        df = df.append(df_temp)
    date = df['date'].values[0]
    date_object = datetime.strptime(date, '%m/%d/%Y').date()
    date = date_object.strftime('%m-%d-%Y')
    df.to_csv(f'ark_holding/Ark_holding_{date}.csv', index=False)
    return date, df

def action(n):
    if n > 0:
        return 'Buy'
    elif n < 0:
        return 'Sell'
    else:
        return 'Hold'

def value_change(x,y, action):
    if action == 'Hold':
        return (y-x)*100/x
    else:
        return None

info = {}
with open('scripts/info.json', 'r') as json_file:
    info = json.load(json_file)
latestfile = info['latest file']
latestdate = latestfile.split('_')[2].split('.')[0]

currentdate, df_current = getHolding()
if currentdate != latestdate:
    df_prev = pd.read_csv(f'ark_holding/{latestfile}')
    df_compare = df_prev.merge(df_current,
                               how='left',
                               left_on=['fund', 'company', 'ticker','cusip'],
                               right_on=['fund', 'company', 'ticker', 'cusip'])
    date = [i for i in df_compare['date_y'].unique() if not pd.isna(i)][0]
    df_compare['date_y'] = df_compare['date_y'].fillna(date)
    df_compare['shares_y'] = df_compare['shares_y'].fillna(0)
    df_compare['market value($)_y'] = df_compare['market value($)_y'].fillna(0)
    df_compare['weight(%)_y'] = df_compare['weight(%)_y'].fillna(0)
    df_compare['shares'] = df_compare['shares_y'] - df_compare['shares_x']
    df_compare['action'] = df_compare.apply(lambda x: action(x['shares']), axis=1)
    df_compare['% change'] = df_compare.apply(lambda x: value_change(x['market value($)_x'],
                                                                     x['market value($)_y'],
                                                                     x['action']), axis=1)
    df_compare['shares'] = abs(df_compare['shares'])
    df_compare = df_compare.rename(columns={'date_y': 'date',
                                            'shares_y': 'holding',
                                            'weight(%)_y': 'weight(%)',
                                            'market value($)_y': 'market value($)'})
    df_compare['date'] = pd.to_datetime(df_compare['date'])
    date = df_compare['date'].dt.strftime('%m-%d-%Y').values[0]
    df_compare['No.'] = df_compare.index + 1
    df_compare[['No.', 'date', 'fund', 'company', 'ticker',
                'holding', 'market value($)', 'weight(%)', 'action',
                'shares', '% change']].to_csv(f'ark_trading/Ark_trade_{date}.csv', index=False)


    latestfile = {"latest file": f'Ark_holding_{date}.csv'}
    with open('scripts/info.json', 'w') as json_file:
        json_file.write(json.dumps(latestfile, indent=4))
else:
    print('No trading output')

def sortFiles():
    files = glob.glob('ark_trading/*.csv')
    dates = [i.split('_')[3].split('.')[0] for i in files]
    dates = [datetime.strptime(i, '%m-%d-%Y').date() for i in dates]
    dates.sort(reverse=True)
    dates = [i.strftime('%m-%d-%Y') for i in dates]
    files = [f'ark_trading/Ark_trade_{i}.csv' for i in dates]
    return files

files = sortFiles()
df = pd.read_csv(files[0])
for i in range(1, len(files)):
    df_temp = pd.read_csv(files[i])
    df = df.append(df_temp)
df_trades = df[df['action'] != 'Hold']
df_trades[['date', 'fund', 'company', 'ticker','holding', 'market value($)',
   'weight(%)', 'action', 'shares']].to_csv('trades.csv', index=False)
df_hold = df[df['action'] == 'Hold']
df_hold[['date', 'fund', 'company', 'ticker','holding', 'market value($)',
   'weight(%)', '% change']].to_csv('holdings.csv', index=False)


