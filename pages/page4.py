# Import required libraries
from dash import html, dcc
from d_code import d
# from Data_cleaning_SQL import df_raw
# from callbacks_new import df_raw
#
#
# filtered_df = df_raw.loc[df_raw['STAGE'] == 'ATE']

colors = {
    # For black background
    # 'background': 'rgb(50, 50, 50)',
    # 'text':  'white'      # '#7FDBFF'
    'background': 'white',
    'text':  'black'      # '#7FDBFF'
}


# Create an app layout instead of app.layout we are using layout for multiple pages
layout = html.Div(style={'backgroundColor': colors['background']}, children=[
    html.Div([
        # html.Div([
              # html.H1('', style={'color': colors['text']},
              #         className='title'), ], className='logo_title'),
        html.Div([
            html.P('Select Product',
                   style={'color': colors['text']},
                   className='drop_down_list_title'
                   ),
            dcc.Dropdown(id='Product_all',
                         # options=[{'label': i, 'value': i}for i in filtered_df['PRODUCT_NAME'].unique()],
                         clearable=True,
                         value=None,
                         placeholder='Select a product here',
                         searchable=True,
                         className='drop_down_list'),
        ], className='title_drop_down_list'),
        html.Div([
            html.P('Select Month',
                   style={'color': colors['text']},
                   className='drop_down_month_title'
                   ),
            dcc.Dropdown(id='Month_all',
                         # options=[{'label': i, 'value': i}for i in df_raw['MONTH'].unique()],
                         # options=[{'label': i, 'value': i} for i in final_month_list],
                         clearable=True,
                         value=None,
                         # multi=True,
                         placeholder='Select a month here',
                         searchable=True,
                         className='drop_down_month'),
        ], className='month_drop_down_list'),
        html.Div([
            html.P('Select Design',
                   style={'color': colors['text']},
                   className='drop_down_month_title'
                   ),
            dcc.Dropdown(id='Design',
                         # options=[{'label': i, 'value': i}for i in df_raw['MONTH'].unique()],
                         # options=[{'label': i, 'value': i} for i in final_month_list],
                         clearable=False,
                         value='all_values',
                         # multi=True,
                         placeholder='Select a design here',
                         searchable=True,
                         className='drop_down_month'),
        ], className='month_drop_down_list'),], className='title_and_drop_down_list'),
    html.Br(),
    html.Br(),
    html.Div([
        html.Div([html.H6(children='Tested',
                          style={'textAlign': 'center', 'color': 'white', 'fontSize': 20}),
                  html.P(id='tested_all',
                         style={
                             'textAlign': 'center', 'color': 'orange', 'fontSize': 40,
                             'margin-top': '-18px'})], className="card_container two columns",),
        html.Div([
            html.H6(children='Pass',
                    style={
                        'textAlign': 'center',
                        'color': 'white', 'fontSize': 20}
                    ),
            html.P(id='pass_all',
                   style={
                       'textAlign': 'center',
                       'color': 'lime', 'margin-top': '-18px',
                       'fontSize': 40}
                   )], className="card_container two columns",),
        html.Div([
            html.H6(children='Fail',
                    style={
                        'textAlign': 'center',
                        'color': 'white', 'fontSize': 20}
                    ),
            html.P(id='fail_all',  # f"{covid_data_1['recovered'].iloc[-1]:,.0f}"
                   style={
                       'textAlign': 'center',
                       'color': 'red', 'margin-top': '-18px',
                       'fontSize': 40}
                   )], className="card_container two columns",),
        html.Div([
            html.H6(children='FTY',
                    style={
                        'textAlign': 'center',
                        'color': 'white', 'fontSize': 20}
                    ),
            html.P(id='fty_all',  # f"{covid_data_1['active'].iloc[-1]:,.0f}"
                   style={
                       'textAlign': 'center',
                       'color': '#e55467', 'margin-top': '-18px',
                       'fontSize': 40}
                   )], className="card_container two columns",),
        html.Div([
            html.H6(children='DPT',
                    style={
                        'textAlign': 'center',
                        'color': 'white', 'fontSize': 20}
                    ),
            html.P(id='dpt_all',  # f"{covid_data_1['active'].iloc[-1]:,.0f}"
                   style={
                       'textAlign': 'center',
                       'color': 'aqua', 'margin-top': '-18px',
                       'fontSize': 40}
                   )], className="card_container two columns")
    ], className="row flex-display"),
html.Div([
                    html.H6('SUMMARY HIGHLIGHTS',
                           style={'color': colors['text'], 'textAlign': 'left'},
                           ),]),
html.Div(id='Summary_highlight', style={'margin-top': '0px', 'padding': '0'}),

    html.Div([
        html.Div([
                    html.H6('Set Month Range for Graphs using Slider',
                           style={'color': colors['text'], 'textAlign': 'center'},
                           className='drop_down_list_title'
                           ),]),
            # dcc.RangeSlider(
            #     id='month-slider',
            #     marks={i: month for i,month in enumerate(d['MONTH'].unique())},
            #     min=0,
            #     max=len(d['MONTH'].unique()) - 1,
            #     value=[0, len(d['MONTH'].unique()) - 1],
            #     # value = [d['MONTH'].iloc[0], d['MONTH'].iloc[-1]],
            #     step=3,
            #     updatemode='drag'
            # ),
            dcc.RangeSlider(
                id='month-range-slider',
                step=1,
                min=0,
                max=0,  # Placeholder
                value=[0, 0],  # Placeholder
                marks={},
                updatemode='drag'
            )
    ]),

    html.Div([
        html.Div([
            dcc.Graph(id='Overall_graph',
                      config={'displayModeBar': 'hover'}),
        ], className="create_container five-half columns"),
        html.Div([
            dcc.Graph(id="DPT_comparison")
        ], className="create_container five-half columns"),
    ], className="row flex-display"),
    html.Div([
        html.Div([
            dcc.Graph(id="Assembly_component_bar")
        ], className="create_container five-half columns"),
        html.Div([
            dcc.Graph(id='Sunburst',
                      config={'displayModeBar': 'hover'}),
        ], className="create_container five-half columns"),
    ], className="row flex-display"),
    ])

