import os
import pandas as pd


def filtering_excluded_ids(baseline_ids_path, source_path):
    filenames = []
    processed_dataframes = []

    ids_to_exclude = set(pd.read_excel(baseline_ids_path, header=None).iloc[:, 0].astype(str))

    for filename in os.listdir(source_path):
        if filename.endswith('.csv') and not filename.endswith('_no_headers.csv'):
            file_path = os.path.join(source_path, filename)
            df = pd.read_csv(file_path, sep=',')

            if 'participant_identifier' not in df.columns:
                print(f"participant_identifier not found in dataframe {filename}")
                continue

            df['participant_identifier'] = df['participant_identifier'].str.strip()
            df = df[~df['participant_identifier'].isin(ids_to_exclude)]

            processed_dataframes.append(df)
            filenames.append(filename)

            out_path_headers = os.path.join(source_path, f"ITEM_{filename}")
            df.to_csv(out_path_headers, index=False)

    return processed_dataframes, filenames
