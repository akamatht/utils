import concurrent.futures
from datetime import datetime
import pandas as pd
import dash
from dash import html, dcc, html, Output, Input
import plotly.graph_objects as go
import json
from kafka import KafkaConsumer
import uuid
import requests
import rx
from pandas.tseries.offsets import BDay
from rx.scheduler import ThreadPoolScheduler


KAFKA_BOOTSTRAP_SERVERS = [
    'njxmd01.hetco.com:9092'
]


def get_kafka_consumer():
    return KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, group_id="tst" + str(uuid.uuid4()))


DEV = True
kafka_topic = 'local-live-fairvalue-prices'
today = datetime.today().strftime('%Y%m%d')
prev_day = (datetime.today() - BDay(1)).strftime('%Y%m%d')
print(today, prev_day)


def get_data(observer, scheduler):
    kafka_consumer = get_kafka_consumer()
    kafka_consumer.subscribe(topics=[kafka_topic])
    start_time = datetime.now().timestamp()
    print(f'start_time {start_time}')

    received_msgs = {}
    num_msgs = 0
    for msg in kafka_consumer:
        msg_stamp = msg.timestamp / 1000
        key = msg.key.decode('utf-8')
        # if not key.startswith("RB"):
        #     continue
        received_prices = msg.value.decode('utf-8')
        msg_dict = json.loads(received_prices)
        fv = {"instr": key, "bid": msg_dict['BidPr'], "ask": msg_dict['AskPr'],"mid": (msg_dict['BidPr'] + msg_dict['AskPr'])/2,
         "book_type": msg_dict['BookType']}
        observer.on_next(fv)
        # print(f'received {received_msgs[key]}')
        num_msgs += 1
        if num_msgs % 500 == 0:
            print(f'read {num_msgs}')
        # if num_msgs == 10_000:
        #     observer.on_completed('Done')


def get_settles_df(symbol:str, exchange:str, date:str) -> pd.DataFrame:
    isodate = datetime.strptime(date,'%Y%m%d').strftime('%Y-%m-%d')
    url = f"http://settles-api.mosaic.hartreepartners.com/settles/api/v1/getFutureCurveSettlement/{symbol}/{exchange}/{isodate}?allow_indicative=true"
    data = requests.get(url)
    df = pd.DataFrame(data=data.json())
    return df[['instrument_key','expiration_date','value']].reset_index(drop=True)



def build_settles_df():
    global DF_SETTLES
    df_cme = get_settles_df("CL,NG,HO,RB", "CME", prev_day)
    df_ice = get_settles_df("B,G", "ICE", prev_day)
    df_settles = pd.concat([df_cme,df_ice], axis=0)
    df_settles = df_settles.rename(columns={'instrument_key': 'instr'})
    df_settles[['symbol', 'contract']] = df_settles['instr'].str.split(expand=True)
    DF_SETTLES = df_settles.copy()
    return df_settles


app = dash.Dash(__name__)

LATEST_SYMBOL = "CL"

DF_SETTLES = build_settles_df()
DF_SETTLES_VIEW = DF_SETTLES.copy()
DF_FV = pd.DataFrame(data=None, columns=['instr','mid'])
fv_msg_collection = {}


@app.callback(
    Output('live-fv-prices', 'figure'),
    Input('symbol-dropdown', 'value')
)
def update_figure(value):
    global LATEST_SYMBOL, DF_SETTLES_VIEW, DF_SETTLES
    global fv_msg_collection
    LATEST_SYMBOL = value
    DF_SETTLES_VIEW = DF_SETTLES[DF_SETTLES['symbol']==value].copy()
    # fig = px.line(DF_SETTLES_VIEW, x="instr", y="value")
    fig = go.Figure()
    fig.add_scatter(x = DF_SETTLES_VIEW['instr'], y=DF_SETTLES_VIEW['value'], mode="lines", name="settles")
    if(len(fv_msg_collection.keys()) != 0):
        df_fv = pd.DataFrame(fv_msg_collection.values())
        print(f'fv msgs {len(df_fv)}')
        df = pd.merge(DF_SETTLES, df_fv, how="left", on="instr")
        df_merged = df[df['symbol']==value].copy()
        df_merged = df_merged.where(pd.notna(df_merged), None)
        print(f'fv msgs after filtering {len(df_merged)} for {value}')
        fig.add_scatter(x=df_merged['instr'], y=df_merged['mid'], mode='lines', name="fv")
        df_implied = df_merged[df_merged['book_type']=='Implied'].copy()
        df_interpolated = df_merged[df_merged['book_type']=='Interpolated'].copy()
        df_extrapolated = df_merged[df_merged['book_type']=='Extrapolated'].copy()
        fig.add_scatter(x=df_implied['instr'], y=df_implied['mid'], mode='markers', name='implied')
        fig.add_scatter(x=df_interpolated['instr'], y=df_interpolated['mid'], mode='markers', name='interpolated')
        fig.add_scatter(x=df_extrapolated['instr'], y=df_extrapolated['mid'], mode='markers', name='extrapolated')

    return fig


app.layout = html.Div([
    dcc.Dropdown(['CL', 'RB', 'HO','B','G','NG'], 'CL', id='symbol-dropdown'),
    dcc.Graph(
        id='live-fv-prices'
    )
])


def subscribe_to_data():
    global fv_msg_collection
    def collect_data(msg_dict:dict):
        global fv_msg_collection
        key = msg_dict['instr']
        fv_msg_collection[key] = msg_dict.copy()

    observable = rx.create(get_data)
    observable.subscribe(on_next=collect_data, on_completed=lambda x: print('On completed'),
                         scheduler=ThreadPoolScheduler())


if __name__ == '__main__':
    # build_settles_df()
    # executor = concurrent.futures.ProcessPoolExecutor(max_workers=2)
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
    executor.submit(subscribe_to_data)
    app.run_server()
