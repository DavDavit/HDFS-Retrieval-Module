def filter_ids(df, x, y, z):
    """
    Filter rows from the DataFrame based on x, y, z criteria.
    """
    filtered_df = df[(df['x_col'] == x) & (df['y_col'] == y) & (df['z_col'] == z)]
    return filtered_df['id'].tolist()
