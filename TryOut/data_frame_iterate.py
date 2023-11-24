# for row in df.itertuples(index=True):
#             ic(row)
#
#
#         tmp_df = df.loc[df['code'] == 'Catalog']
#         while not tmp_df.empty:
#             t_id = tmp_df.iloc[0]['ID_tblDirectoryItem']
#             ic(t_id)
#             tmp_df = df.loc[df['ID_parent'] == t_id]