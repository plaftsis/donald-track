from dash.dependencies import Input, Output
from pymongo import MongoClient
from nltk.probability import FreqDist
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from textblob import TextBlob
from dotenv import load_dotenv
import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly.graph_objs as go
import itertools
import math
import os
import datetime
import re
import nltk

START_COLOR = '#FEFEFE'
POSITIVE_COLOR = '#8CC152'
NEGATIVE_COLOR = '#DA4453'
NEUTRAL_COLOR = '#F6BB42'

nltk.download('punkt')
nltk.download('stopwords')

load_dotenv()

db_host = os.environ['DB_HOST']
db_port = int(os.environ['DB_PORT'])
db_name = os.environ['DB_NAME']
db_collection = os.environ['DB_COLLECTION']

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = 'donald-track'

server = app.server

app.layout = html.Div(children=[
    # Header
    html.Header(
        children=[
            html.Img(
                src='https://trumpoji.com/images/emojis/peering_256.png',
                alt='donald-track',
                width='200px'
            ),
            html.H1(
                'donald-track'
            ),
            html.H3(
                'Tracking tweets about Donald Trump in real-time 🔥'
            )
        ],
        style=dict(padding='25px', textAlign='center')
    ),

    # Graphs
    html.Div(id='top-section'),
    html.Div(id='bottom-section'),

    # Credits
    html.Footer(
        className='text-lg-start bg-light text-muted',
        children=[
            html.Section(
                children=[
                    html.Div(
                        className='container-flouid',
                        children=[
                            html.Div(
                                className='row mt-3',
                                children=[
                                    html.Div(
                                        className='col-md-4 col-lg-4 col-xl-4',
                                        children=[
                                            html.H6(
                                                'About'
                                            ),
                                            html.P(
                                                'This project was created for the postgraduate course M151: Web Systems and Applications, at the Department of Informatics and Telecommunications within the University of Athens.'
                                            )
                                        ],
                                        style=dict(marginBottom='10px')
                                    ),
                                    html.Div(
                                        className='col-md-2 col-lg-2 col-xl-2',
                                        children=[
                                            html.H6(
                                                'Data extracted from'
                                            ),
                                            html.A(
                                                'Twitter API',
                                                href='https://developer.twitter.com'
                                            )
                                        ],
                                        style=dict(marginBottom='10px')
                                    ),
                                    html.Div(
                                        className='col-md-2 col-lg-2 col-xl-2',
                                        children=[
                                            html.H6(
                                                'Code available at'
                                            ),
                                            html.A(
                                                'GitHub',
                                                href='https://github.com/plaftsis/m151'
                                            )
                                        ],
                                        style=dict(marginBottom='10px')
                                    ),
                                    html.Div(
                                        className='col-md-2 col-lg-2 col-xl-2',
                                        children=[
                                            html.H6(
                                                'Made with'
                                            ),
                                            html.A(
                                                'Apache Kafka',
                                                href='https://kafka.apache.org/',
                                                style=dict(display='block')
                                            ),
                                            html.A(
                                                'MongoDB',
                                                href='https://www.mongodb.com/',
                                                style=dict(display='block')
                                            ),
                                            html.A(
                                                'Pandas',
                                                href='https://pandas.pydata.org/',
                                                style=dict(display='block')
                                            ),
                                            html.A(
                                                'Dash Plotly',
                                                href='https://plotly.com/dash/',
                                                style=dict(display='block')
                                            )
                                        ],
                                        style=dict(marginBottom='10px')
                                    ),
                                    html.Div(
                                        className='col-md-2 col-lg-2 col-xl-2',
                                        children=[
                                            html.H6(
                                                'Author'
                                            ),
                                            html.A(
                                                'Paris Laftsis',
                                                href='https://www.linkedin.com/in/paris-laftsis/'
                                            )
                                        ],
                                        style=dict(marginBottom='10px')
                                    )
                                ],
                                style=dict(padding='25px')
                            )
                        ]
                    )
                ]
            )
        ], style=dict(padding='25px', fontSize='16px')
    ),

    # Update
    dcc.Interval(
        id='interval-component-fast',
        interval=2000,
        n_intervals=0
    )
])


@app.callback(Output('top-section', 'children'),
              [Input('interval-component-fast', 'n_intervals')])
def update_top_section_live(n):
    # Loading data from MongoDB
    client = MongoClient(db_host, db_port)
    db = client[db_name]
    df = pd.DataFrame(list(db[db_collection].find()))

    # Convert string to datetime
    df['created_at'] = pd.to_datetime(df['created_at'])
    df['user_created_at'] = pd.to_datetime(df['user_created_at'])

    # Clean and transform data to enable time series
    result = df.groupby([pd.Grouper(key='created_at', freq='5s'), 'polarity']) \
        .count().unstack(fill_value=0).stack().reset_index()
    result = result.rename(columns={"_id": "number_of_tweets"})
    time_series = result["created_at"][result['polarity'] == 0].reset_index(drop=True)

    min30 = datetime.datetime.utcnow() - datetime.timedelta(minutes=30)

    neutral_num = result[result['created_at'] > min30]["number_of_tweets"][result['polarity'] == 0].sum()
    negative_num = result[result['created_at'] > min30]["number_of_tweets"][result['polarity'] == -1].sum()
    positive_num = result[result['created_at'] > min30]["number_of_tweets"][result['polarity'] == 1].sum()

    # Create the graph
    children = [
        html.Div(
            className='container-fluid',
            children=[
                html.Div(
                    className='row',
                    children=[
                        html.Div(
                            className='col-md-8 col-lg-8 col-xl-8 mx-auto',
                            children=[
                                # Line Chart
                                html.Div([
                                    dcc.Graph(
                                        id='line-chart',
                                        figure={
                                            'data': [
                                                go.Scatter(
                                                    x=time_series,
                                                    y=result["number_of_tweets"][result['polarity'] == 0].reset_index(
                                                        drop=True),
                                                    name="Neutrals",
                                                    opacity=0.8,
                                                    mode='lines',
                                                    line=dict(width=0.5, color=NEUTRAL_COLOR),
                                                    stackgroup='one'
                                                ),
                                                go.Scatter(
                                                    x=time_series,
                                                    y=result["number_of_tweets"][result['polarity'] == -1].reset_index(
                                                        drop=True).apply(
                                                        lambda x: -x),
                                                    name="Negatives",
                                                    opacity=0.8,
                                                    mode='lines',
                                                    line=dict(width=0.5, color=NEGATIVE_COLOR),
                                                    stackgroup='two'
                                                ),
                                                go.Scatter(
                                                    x=time_series,
                                                    y=result["number_of_tweets"][result['polarity'] == 1].reset_index(
                                                        drop=True),
                                                    name="Positives",
                                                    opacity=0.8,
                                                    mode='lines',
                                                    line=dict(width=0.5, color=POSITIVE_COLOR),
                                                    stackgroup='three'
                                                )
                                            ],
                                            'layout': {
                                                'title': 'User opinions'
                                            }
                                        }
                                    )
                                ])
                            ]
                        ),
                        html.Div(
                            className='col-md-4 col-lg-4 col-xl-4 mx-auto',
                            children=[
                                # Pie Chart
                                html.Div([
                                    dcc.Graph(
                                        id='pie-chart',
                                        figure={
                                            'data': [
                                                go.Pie(
                                                    labels=['Positives', 'Negatives', 'Neutrals'],
                                                    values=[positive_num, negative_num, neutral_num],
                                                    name="View Metrics",
                                                    opacity=0.7,
                                                    marker_colors=[POSITIVE_COLOR, NEGATIVE_COLOR, NEUTRAL_COLOR],
                                                    textinfo='value',
                                                    hole=0.65)
                                            ],
                                            'layout': {
                                                'showlegend': False,
                                                'title': 'Tweets in last 30 minutes',
                                                'annotations': [
                                                    dict(
                                                        text='{0:.1f}K'.format(
                                                            (positive_num + negative_num + neutral_num) / 1000),
                                                        font=dict(size=40),
                                                        showarrow=False
                                                    )
                                                ]
                                            }

                                        }
                                    )
                                ])
                            ]
                        )
                    ]
                )
            ]
        )
    ]
    return children


@app.callback(Output('bottom-section', 'children'),
              [Input('interval-component-fast', 'n_intervals')])
def update_bottom_section_live(n):
    # Loading data from MongoDB
    client = MongoClient(db_host, db_port)
    db = client[db_name]
    df = pd.DataFrame(list(db[db_collection].find()))

    # Convert string to datetime
    df['created_at'] = pd.to_datetime(df['created_at'])
    df['user_created_at'] = pd.to_datetime(df['user_created_at'])

    # Clean and transform data to enable word frequency
    content = ' '.join(df['text'])
    content = re.sub(r'http\S+', '', content)
    content = content.replace('RT ', ' ').replace('&amp;', 'and')
    content = re.sub('[^A-Za-z0-9]+', ' ', content)
    content = content.lower()

    # Filter constants for states in US
    STATES = ['Alabama', 'AL', 'Alaska', 'AK', 'American Samoa', 'AS', 'Arizona', 'AZ', 'Arkansas', 'AR', 'California',
              'CA', 'Colorado', 'CO', 'Connecticut', 'CT', 'Delaware', 'DE', 'District of Columbia', 'DC',
              'Federated States of Micronesia', 'FM', 'Florida', 'FL', 'Georgia', 'GA', 'Guam', 'GU', 'Hawaii', 'HI',
              'Idaho', 'ID', 'Illinois', 'IL', 'Indiana', 'IN', 'Iowa', 'IA', 'Kansas', 'KS', 'Kentucky', 'KY',
              'Louisiana', 'LA', 'Maine', 'ME', 'Marshall Islands', 'MH', 'Maryland', 'MD', 'Massachusetts', 'MA',
              'Michigan', 'MI', 'Minnesota', 'MN', 'Mississippi', 'MS', 'Missouri', 'MO', 'Montana', 'MT', 'Nebraska',
              'NE', 'Nevada', 'NV', 'New Hampshire', 'NH', 'New Jersey', 'NJ', 'New Mexico', 'NM', 'New York', 'NY',
              'North Carolina', 'NC', 'North Dakota', 'ND', 'Northern Mariana Islands', 'MP', 'Ohio', 'OH', 'Oklahoma',
              'OK', 'Oregon', 'OR', 'Palau', 'PW', 'Pennsylvania', 'PA', 'Puerto Rico', 'PR', 'Rhode Island', 'RI',
              'South Carolina', 'SC', 'South Dakota', 'SD', 'Tennessee', 'TN', 'Texas', 'TX', 'Utah', 'UT', 'Vermont',
              'VT', 'Virgin Islands', 'VI', 'Virginia', 'VA', 'Washington', 'WA', 'West Virginia', 'WV', 'Wisconsin',
              'WI', 'Wyoming', 'WY']
    STATE_DICT = dict(itertools.zip_longest(*[iter(STATES)] * 2, fillvalue=''))
    INV_STATE_DICT = dict((v, k) for k, v in STATE_DICT.items())

    # Clean and transform data to enable geo-distribution
    is_in_US = []
    df = df.fillna(' ')
    for x in df['user_location']:
        check = False
        for s in STATES:
            if s in x:
                is_in_US.append(STATE_DICT[s] if s in STATE_DICT else s)
                check = True
                break
        if not check:
            is_in_US.append(None)

    geo_dist = pd.DataFrame(is_in_US, columns=['state']).dropna().reset_index()
    geo_dist = geo_dist.groupby('state').count().rename(columns={'index': 'number'}) \
        .sort_values(by=['number'], ascending=False).reset_index()
    geo_dist['log_num'] = geo_dist['number'].apply(lambda x: math.log(x, 2))
    geo_dist['full_state_name'] = geo_dist['state'].apply(lambda x: INV_STATE_DICT[x])
    geo_dist['text'] = geo_dist['full_state_name'] + '<br>' + 'Num: ' + geo_dist['number'].astype(str)

    tokenized_word = word_tokenize(content)
    stop_words = set(stopwords.words('english'))
    filtered_sent = []
    for w in tokenized_word:
        if (w not in stop_words) and (len(w) >= 3):
            filtered_sent.append(w)
    fdist = FreqDist(filtered_sent)
    fd = pd.DataFrame(fdist.most_common(16), columns=['word', 'frequency']).drop([0]).reindex()
    fd['polarity'] = fd['word'].apply(lambda x: TextBlob(x).sentiment.polarity)
    fd['marker_color'] = fd['polarity'].apply(
        lambda x: NEGATIVE_COLOR if x < -0.1 else (POSITIVE_COLOR if x > 0.1 else NEUTRAL_COLOR)
    )
    fd['line_color'] = fd['polarity'].apply(
        lambda x: NEGATIVE_COLOR if x < -0.1 else (POSITIVE_COLOR if x > 0.1 else NEUTRAL_COLOR)
    )

    # Create the graph
    children = [
        html.Div(
            className='container-fluid',
            children=[
                html.Div(
                    className='row',
                    children=[
                        html.Div(
                            className='col-md-6 col-lg-6 col-xl-6 mx-auto',
                            children=[
                                # Bar Chart
                                dcc.Graph(
                                    id='bar-chart',
                                    figure={
                                        'data': [
                                            go.Bar(
                                                x=fd["frequency"].loc[::-1],
                                                y=fd["word"].loc[::-1],
                                                name="Neutrals",
                                                orientation='h',
                                                marker_color=fd['marker_color'].loc[::-1],
                                                marker=dict(
                                                    line=dict(
                                                        color=fd['line_color'].loc[::-1],
                                                        width=1),
                                                ),
                                                opacity=0.7
                                            )
                                        ],
                                        'layout': {
                                            'title': 'Top related topics',
                                            'hovermode': 'closest'
                                        }
                                    }
                                )
                            ]
                        ),
                        html.Div(
                            className='col-md-6 col-lg-6 col-xl-6 mx-auto',
                            children=[
                                # Map
                                dcc.Graph(
                                    id='y-time-series',
                                    figure={
                                        'data': [
                                            go.Choropleth(
                                                locations=geo_dist['state'],
                                                z=geo_dist['log_num'].astype(float),
                                                locationmode='USA-states',
                                                text=geo_dist['text'],
                                                geo='geo',
                                                colorbar_title='Num in Log2',
                                                marker_line_color=START_COLOR,
                                                colorscale=[START_COLOR, NEUTRAL_COLOR]
                                            )
                                        ],
                                        'layout': {
                                            'title': "Geographic segmentation for US",
                                            'geo': {'scope': 'usa'}
                                        }
                                    }
                                )
                            ]
                        )
                    ]
                )
            ]
        )
    ]

    return children


if __name__ == '__main__':
    app.run_server()
