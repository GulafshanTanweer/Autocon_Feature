from pathlib import Path
from typing import Iterable, Any
import os
import pandas as pd
from pdfminer.high_level import extract_pages
from tqdm import tqdm
from joblib import Parallel, delayed
import numpy as np
import re
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthenticator
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
# Define the Cassandra connection details
contact_points = ['192.168.32.82']
port = 9042
username = 'cassautocon'
password = 'cassautocon'
keyspace = 'autocondb'


def show_ltitem_hierarchy(o: Any, coordinates_data, depth=0):
    """Show location and text of LTItem and all its descendants"""
    # if depth == 0:
        # print('element                        x1  y1  x2  y2   text   font')
        # print('------------------------------ --- --- --- ---- -----  ------')

    if depth <= 2:
        final_row = [get_indented_name(o, depth).strip()]
        final_row.extend(get_optional_bbox(o))
        final_row.append(get_optional_text(o).strip())
        final_row.append(get_optional_fontinfo(o))
        if len(final_row) == 7:
            coordinates_data.loc[coordinates_data.shape[0]] = final_row

        if isinstance(o, Iterable):
            for i in o:
                show_ltitem_hierarchy(i, coordinates_data, depth=depth + 1)
    


def get_indented_name(o: Any, depth: int) -> str:
    """Indented name of LTItem"""
    return '  ' * depth + o.__class__.__name__


def get_optional_bbox(o: Any) -> str:
    """Bounding box of LTItem if available, otherwise empty string"""
    if hasattr(o, 'bbox'):
        return [i for i in o.bbox]
    return ''


def get_optional_fontinfo(o: Any) -> str:
    if hasattr(o, 'fontname') and hasattr(o, 'size'):
        return o.size
    return ''


def get_optional_text(o: Any) -> str:
    """Text of LTItem if available, otherwise empty string"""
    if hasattr(o, 'get_text'):
        return o.get_text().strip()
    return ''


final_data = pd.DataFrame(columns=['Part Serial N°', 'Part Description', 'Part N°', 'Drawing N°',
       'CA code and s/n', 'Quantity', 'Attached\ndocuments', 'Reference',
       'MSN', 'A/C Type', 'Page', 'Suffix', 'Description of Divergence', 'Revision', 'path'])
main_path = r'/home/shreyaskumar/Documents/SPT'
not_parsed = []
# for curr_path in tqdm(os.listdir(main_path)):
#     # print(curr_path)
# curr_path = 'SNA-005630728.pdf'
#     if curr_path.endswith('.pdf')
def get_data(final_path):
    path = Path(final_path).expanduser()
    pages = extract_pages(path)
    coordinates_data = pd.DataFrame(columns=['element', 'x1', 'y1', 'x2', 'y2', 'text', 'font'])
    show_ltitem_hierarchy(o=pages, coordinates_data=coordinates_data)
    text_data = coordinates_data[coordinates_data['element'] == 'LTTextBoxHorizontal']
    text_data = text_data.loc[:coordinates_data[coordinates_data['element'] == 'LTPage'].index[1], :]
    text_data = text_data.drop_duplicates()
    text_data = text_data.sort_values(by=['x1', 'y1'])
    text_data.reset_index(drop=True, inplace=True)
    text_data[text_data['y1'] > 150]
    text_data = text_data[text_data['y1'] > 150]
    value_data = text_data['x1'].value_counts()
    value_data = value_data.reset_index()
    value_count_data = value_data.sort_values(by='index')
    value_count_data.reset_index(drop=True, inplace=True)
    value_count_data['diff'] = value_count_data.index.map(lambda i: value_count_data.loc[i, 'index'] - value_count_data.loc[i - 1, 'index'] if i > 0 else np.nan)
    value_count_data['col'] = value_count_data.index
    for i, row in value_count_data.iterrows():
        if not pd.isna(row['diff']):
            if row['diff'] < 5:
                value_count_data.loc[i, 'col'] = value_count_data.loc[i - 1, 'col']
    value_count_data.columns = ['x1', 'count', 'diff', 'column_no']
    text_data = text_data.merge(value_count_data[['x1', 'column_no']], on='x1')
    curr_df = pd.DataFrame(columns=['key', 'value'])
    drop_index = []
    for i, row in text_data.iterrows():
        if i not in drop_index:
            value_count_data_data = text_data[text_data['column_no'] > row['column_no']]
            value_count_data_data.reset_index(drop=True, inplace=True)
            y_list = np.array(value_count_data_data['y1'].to_list()) - row['y1']
            value_count_data_data = value_count_data_data.loc[(np.where((y_list < 5) & (y_list > -5)))]
            for j, row1 in value_count_data_data.iterrows():
                if abs(row['x1'] - row1['x1']) < 150:
                    curr_df.loc[curr_df.shape[0]] = [row['text'], row1['text']]
                    drop_index.append(text_data[text_data['text'] == row1['text']].index[0])
                    break
    index = text_data.index.map(lambda i: text_data.loc[i, 'column_no'] if re.search(r'Description\s*of\s*Divergence', text_data.loc[i, 'text']) else np.nan)
    # index_2 = text_data.index.map(lambda i: text_data.loc[i, 'column_no'] if re.search('Location\s*:', text_data.loc[i, 'text']) else np.nan)
    column_list = text_data.loc[index[np.where(~index.isna())], 'column_no'].to_list()
    description_text = text_data[text_data['column_no'].isin(column_list)]['text'].to_list()[::-1]
    curr_df.loc[curr_df.shape[0]] = [description_text[0],'\n\n'.join(description_text[1:]).strip('\n')]
    curr_df = curr_df.T.reset_index(drop=True)
    curr_df.columns = curr_df.iloc[0]
    curr_df = curr_df.iloc[1:]
    curr_df.reset_index(drop=True, inplace=True)
    curr_df['path'] = final_path.split('/')[-1]
    # if 'Revision' no  t in curr_df.columns:
    #     curr_df['Revision'] = np.nan
    # if 'Suffix' not in curr_df.columns:
    #     curr_df['Suffix'] = np.nan
    return curr_df.loc[:, curr_df.columns.isin(final_data.columns)]

all_dfs = Parallel(n_jobs=-1)(delayed (get_data)(os.path.join(main_path, curr_path)) for curr_path in tqdm(os.listdir(main_path)) if curr_path.endswith('.pdf'))
for df in all_dfs:
    final_data = pd.concat([final_data, df], ignore_index=True)
final_data = pd.concat(all_dfs, ignore_index=True)
final_data.to_csv('parsed_pdfs_ANIF.csv', index=False)
for df in all_dfs:
    final_data = pd.concat([final_data, df], ignore_index=True)
final_data = pd.concat(all_dfs, ignore_index=True)
final_data.to_csv('SPT.csv', index=False)
auth_provider = PlainTextAuthProvider(username=username, password=password)
cluster = Cluster(contact_points=contact_points, port=port, auth_provider=auth_provider)
session = cluster.connect(keyspace=keyspace)
print("connected")
print(session.execute("select reference from autocondb.Concession_Document"))

    