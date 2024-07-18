
# Import necessary libraries 
from dash import html, dcc
from dash.dependencies import Input, Output


# Connect to main app.py file
from app import app

# Connect to your app pages
from pages import table, page4
# from pages import page4

# Connect the navbar to the index
from components import navbar

server = app.server

# Define the navbar
nav = navbar.navbar_input()

# Define the index page layout
app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    nav,
    html.Div(id='page-content', children=[]),
    dcc.Interval(id='interval_db', interval=86400000 * 7, n_intervals=0),
])


# Create the callback to handle pages input
@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])
def display_page(pathname):
    # if pathname == '/page1':
    #     return page1.layout
    # if pathname == '/table':
    #     return table.layout
    # if pathname == '/page2':
    #     return page2.layout
    # if pathname == '/Comparison':
    #     return Comparison.layout
    # if pathname == '/page3':
    #     return page3.layout
    if pathname == '/page4':
        return page4.layout
    else:  # if redirected to unknown link
        return page4.layout


# Run the app on localhost:8050
if __name__ == '__main__':
    # app.run_server(host='172.16.89.14', port=8048)  # for making unique id on server
    # app.run_server(host='172.16.21.154', port=8048)  # for making a unique id on my pc
    app.run_server(host='0.0.0.0', port=8050)  # for making a local host with different ips
