from dash import html, dcc
# from Data_cleaning_SQL import df_raw
from callback_analysis import sql_data


df_raw = sql_data('raw')

colors = {
    # For black backgroung
    # 'background': 'rgb(50, 50, 50)',
    # 'text':  'white'      # '#7FDBFF'
    'background': 'white',
    'text':  'black'      # '#7FDBFF'

}
layout = html.Div(style={'backgroundColor': colors['background']}, children=[
    html.Div([
        html.Div([
            html.H1('Faults Data', style={'color': colors['text']},
                    className='title'), ], className='logo_title'),
        html.Div([
            html.P('Select Product',
                   style={'color': colors['text']},
                   className='drop_down_list_title'
                   ),
            dcc.Dropdown(id='Products_table',
                         options=[{'label': i, 'value': i}for i in df_raw['PRODUCT_NAME'].unique()],
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
            dcc.Dropdown(id='Month_table',
                         options=[{'label': i, 'value': i}for i in df_raw['MONTH'].unique()],
                         # options=[{'label': i, 'value': i} for i in final_month_list],
                         multi=True,
                         clearable=True,
                         value=None,
                         placeholder='Select a month here',
                         searchable=True,
                         className='drop_down_month'),
        ], className='month_drop_down_list'), ], className='title_and_drop_down_list'),
    # html.H3('Faults Data'),
    html.Div([
        html.P('Faults list',
               style={'color': colors['text']},
               className='drop_down_month_title'
               ),
        dcc.Dropdown(id='List of Faults',
                     value=None,
                     placeholder='Select a Fault here',
                     searchable=True,
                     className='drop_down_faults'
                     ),
    ], className='month_drop_down_list'),
    html.Br(),
    html.Div(id='Faults_list')
])
