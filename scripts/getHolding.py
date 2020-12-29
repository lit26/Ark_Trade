import pandas as pd
import io
import requests

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
df = pd.DataFrame([], columns=['date','fund','company','ticker','cusip','shares','market value($)','weight(%)'])
for etf in ark_etf:
    s = requests.get(etf['url']).content
    df_temp = pd.read_csv(io.StringIO(s.decode('utf-8'))).dropna()
    df = df.append(df_temp)
date = df['date'].values[0]
date = date.replace('/','-')
df.to_csv(f'ark_holding/Ark_holding_{date}.csv', index=False)