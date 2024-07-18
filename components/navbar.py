# Import necessary libraries
# from dash import html
import dash_bootstrap_components as dbc
from dash import html, dcc
from app import app


# Define the navbar structure
def navbar_input():

    layout = html.Div([
        dcc.Store(id='clear-cache-store', data=0, storage_type='memory'),
        html.Div(id='clear-cache-persistent', style={'display': 'none'}),
        dbc.NavbarSimple(
            children=[
                # dbc.NavItem(dbc.NavLink("SUMMARY", className="summary-link", href="/page1"),
                #             style={'margin-left': '20px', 'verticalAlign': 'top'}),
                # dbc.NavItem(dbc.NavLink("F1_DATA", className="f1-data-link", href="/page2"),
                #             style={'margin-left': '20px', 'verticalAlign': 'top'}),
                # dbc.NavItem(dbc.NavLink("F2_DATA", href="/page3"),
                #             style={'margin-left': '20px', 'verticalAlign': 'top'}),
                dbc.NavItem(dbc.NavLink("Analysis", href="/page4"),
                            style={'margin-left': '20px', 'margin-right': '20px', 'verticalAlign': 'top'}),
                # dbc.NavItem(dbc.NavLink("COMPARISON", href="/Comparison"),
                #             style={'margin-left': '20px', 'verticalAlign': 'top'}),
                # dbc.NavItem(dbc.NavLink("DATA_TABLE", href="/table"),
                #             style={'margin-left': '20px', 'verticalAlign': 'top'}),
                # Button in navbar
                dbc.NavItem(
                    dbc.Button('Cache_clear', id='clear-cache-button', color='primary', n_clicks=0, className='button')
                ),
                # Output div in navbar
                dbc.NavItem(
                    html.Div(id='navbar-output'),
                ),

                html.Img(src=app.get_asset_url('logo.png'), style={'height': '40px', 'margin-left': '60px',
                                                                   'margin-right': '10px'})
            ],
            brand="FAULT ANALYSIS APP",
            brand_href="/page1",
            color="dark",
            dark=True,
            brand_style={'fontWeight': 'bold', 'fontSize': '25px'}
        ),
    ])

    return layout
