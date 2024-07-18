from dash import Dash
import dash_bootstrap_components as dbc
import os
import sys


font_awesome = "https://use.fontawesome.com/releases/v5.10.2/css/all.css"
# meta_tags = [{"name": "viewport", "content": "width=device-width"}]
# external_stylesheets = [meta_tags, font_awesome]


def find_data_file(filename):
    if getattr(sys, 'frozen', False):
        # The application is frozen
        datadir = os.path.dirname(sys.executable)
    else:
        # The application is not frozen
        # Change this bit to match where you store your data files:
        datadir = os.path.dirname(__file__)

    return os.path.join(datadir, filename)


# app = Dash(__name__, external_stylesheets = external_stylesheets, suppress_callback_exceptions=True,
#            assets_folder="D:/Desktop/Tarun/Fault_Analysis/assets")
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP, font_awesome],
           meta_tags=[{"name": "viewport", "content": "width=device-width"}],
           suppress_callback_exceptions=True, assets_folder=find_data_file('assets/'))

