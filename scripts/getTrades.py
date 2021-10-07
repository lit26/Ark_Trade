import json
from datetime import datetime
import pandas as pd
import io
import requests
import quantstats as qs
import glob

arkk_url = "https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_INNOVATION_ETF_ARKK_HOLDINGS.csv"
arkq_url = "https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_INNOVATION_ETF_ARKQ_HOLDINGS.csv"
arkw_url = "https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_INNOVATION_ETF_ARKW_HOLDINGS.csv"
arkg_url = "https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_INNOVATION_ETF_ARKG_HOLDINGS.csv"
arkf_url = "https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_INNOVATION_ETF_ARKF_HOLDINGS.csv"
arkx_url = "https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_INNOVATION_ETF_ARKX_HOLDINGS.csv"
print3d_url = "https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_INNOVATION_ETF_PRNT_HOLDINGS.csv"
israel_url = "https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_INNOVATION_ETF_IZRL_HOLDINGS.csv"

urls = [arkk_url, arkq_url, arkw_url, arkg_url, arkf_url,print3d_url,israel_url]
ark_etf = [{'name': 'ARKK', 'url': arkk_url, 'file': 'ARK_INNOVATION_ETF_ARKK_HOLDINGS.csv'},
           {'name': 'ARKQ', 'url': arkq_url, 'file': 'ARK_INNOVATION_ETF_ARKQ_HOLDINGS.csv'},
           {'name': 'ARKW', 'url': arkw_url, 'file': 'ARK_INNOVATION_ETF_ARKW_HOLDINGS.csv'},
           {'name': 'ARKG', 'url': arkg_url, 'file': 'ARK_INNOVATION_ETF_ARKG_HOLDINGS.csv'},
           {'name': 'ARKF', 'url': arkf_url, 'file': 'ARK_INNOVATION_ETF_ARKF_HOLDINGS.csv'},
           {'name': 'ARKX', 'url': arkx_url, 'file': 'ARK_INNOVATION_ETF_ARKX_HOLDINGS.csv'},
           {'name': 'PRNT', 'url': print3d_url, 'file': 'ARK_INNOVATION_ETF_PRNT_HOLDINGS.csv'},
          {'name': 'IZRL', 'url': israel_url, 'file': 'ARK_INNOVATION_ETF_IZRL_HOLDINGS.csv'}]
ticker_dict = {'TREE UW':'TREE','ARCT UQ':'ARCT','TCS LI':'TCS.IL','TAK UN':'TAK',
              '6618':'6618.HK','8473':'8473.T','3690':'3690.HK','4689':'4689.T',
              '6060':'6060.HK','4477':'4477.T','9923':'9923.HK','ADYEN':'ADYEY',
              'KSPI':'KSPI.IL'}

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) \
            AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36',
          'Upgrade-Insecure-Requests': '1'}

def formatShares(x):
    return float(''.join(x.split(',')))

def getHolding():
    df = pd.DataFrame([], columns=['date','fund','company','ticker','cusip','shares','market value($)','weight(%)'])
    for etf in ark_etf:
        print(f'Fetching: {etf["name"]}, url: {etf["url"]}')
        s = requests.get(etf['url'],headers=headers,cookies={'from-my': 'browser'}).content
        df_temp = pd.read_csv(io.StringIO(s.decode('utf-8'))).dropna()
        df_temp['market value($)'] = df_temp['market value ($)']
        df_temp['weight(%)'] = df_temp['weight (%)']
        df_temp['market value($)'] = df_temp['market value($)'].apply(lambda x: formatShares(x[1:]))
        df['weight(%)'] = df['weight(%)'].apply(lambda x: x[:-1])
        df_temp['shares'] = df_temp['shares'].apply(lambda x: formatShares(x))
        df_temp = df_temp[['date', 'fund', 'company', 'ticker', 'cusip', 'shares', 'market value($)', 'weight(%)']]
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
        if x == 0:
            return 0
        return (y-x)*100/x
    else:
        return None

info = {}
with open('scripts/info.json', 'r') as json_file:
    info = json.load(json_file)
latestfile = info['latest file']
latestdate = latestfile.split('_')[2].split('.')[0]

currentdate, df_current = getHolding()

def combineHolding(df_current):
    df_history = pd.read_csv('holdings.csv')
    df2 = df_current
    df2['date'] = pd.to_datetime(df2['date']).apply(lambda x: x.strftime('%Y-%m-%d'))
    df2 = df2.rename(columns={'shares': 'holding'})
    df_history = df2.append(df_history)
    df_history[['date', 'fund', 'company', 'ticker', 'holding', 'market value($)',
                'weight(%)']].to_csv('holdings.csv', index=False)

def getTrades(currentdate, latestdate, latestfile, df_current):
    if currentdate != latestdate:
        # add to history
        combineHolding(df_current)

        # compare to previous
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
        df_compare['shares'] = df_compare['shares_y'].astype(float) - df_compare['shares_x'].astype(float)
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
        df_compare = df_compare[['No.', 'date', 'fund', 'company', 'ticker',
                    'holding', 'market value($)', 'weight(%)', 'action',
                    'shares', '% change']]
        df_compare.to_csv(f'ark_trading/Ark_trade_{date}.csv', index=False)

        latestfile = {"latest file": f'Ark_holding_{date}.csv'}

        # combine trades
        df_history = pd.read_csv('trades.csv')
        df_temp = df_compare[df_compare['action'] != 'Hold']
        df_temp['date'] = df_temp['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        df_temp = df_temp.append(df_history)
        df_temp[['date', 'fund', 'company', 'ticker', 'holding', 'market value($)',
                 'weight(%)', 'action', 'shares']].to_csv('trades.csv', index=False)
        # df_history = pd.read_csv('holdings.csv')
        # df_temp = df_compare[df_compare['action'] == 'Hold']
        # df_temp['date'] = df_temp['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        # df_temp = df_temp.append(df_history)
        # df_temp[['date', 'fund', 'company', 'ticker', 'holding', 'market value($)',
        #          'weight(%)', '% change']].to_csv('holdings.csv', index=False)
        with open('scripts/info.json', 'w') as json_file:
            json_file.write(json.dumps(latestfile, indent=4))

        getStat(df_compare)
    else:
        print('No trading output')

def getStat(df):
    df = df[df['fund'] != 'PRNT']
    df = df[df['fund'] != 'IZRL']
    tickers = [i for i in list(df['ticker'].unique()) if isinstance(i, str)]
    yf_tickers = [ticker_dict[i] if i in ticker_dict else i.strip() for i in tickers]
    tickers = dict(zip(tickers, yf_tickers))

    for k, v in tickers.items():
        stat = {}
        stock = qs.utils.download_returns(v, '2y')
        stat['sharpe'] = qs.stats.sharpe(stock)
        stat['sortino'] = qs.stats.sortino(stock)
        stat['profit_factor'] = qs.stats.profit_factor(stock)
        stat['max_drawdown'] = qs.stats.max_drawdown(stock)
        stat['risk_return_ratio'] = qs.stats.risk_return_ratio(stock)
        stat['win_loss_ratio'] = qs.stats.win_loss_ratio(stock)
        stat['win_rate'] = qs.stats.win_rate(stock)

        for key, val in stat.items():
            stat[key] = round(val, 2)

        daily_return = list(stock.values)[1:]
        daily_return = [round(i * 100, 1) for i in daily_return]
        stat['daily_return'] = daily_return
        with open(f'stat/{k}.json', 'w') as outfile:
            json.dump(stat, outfile)

getTrades(currentdate, latestdate, latestfile, df_current)






