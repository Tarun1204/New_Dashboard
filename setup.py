# from setuptools import find_packages
from cx_Freeze import setup, Executable

import opcode
import os

# opcode is not a virtualenv module, so we can use it to find the stdlib; this is the same
# trick used by distutils itself it installs itself into the virtualenv
distutils_path = os.path.join(os.path.dirname(opcode.__file__), 'distutils')
build_exe_options = {'include_files': [(distutils_path, 'distutils')], "excludes": ["distutils"]}

# options = {
#     'build_exe': {
#         'includes': [
#             'cx_Logging', 'idna',
#         ],
#         'packages': [
#             'asyncio', 'flask', 'jinja2', 'dash', 'plotly', 'waitress'
#         ],
#         'excludes': ['tkinter']
#     }
# }

executables = [
    Executable('server.py',
               base='console',
               targetName='module_analysis.exe')
]

setup(
    name='module_analysis',
    # packages=find_packages(),
    version='0.1',
    description='rig',
    executables=executables,
    options={"build_exe": build_exe_options},
    include_package_data=True
)
