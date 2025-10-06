import os
import pandas as pd


def assessment_window_filtering(file_map, source_path):

    window_map = {
        'Baseline': 'Baseline',
        '2-month post-baseline': 'T1',
        '6-month post-baseline': 'T2',
        '12-month post-baseline': 'T3'
    }
    code_map = {
        'Baseline': 0,
        '2-month post-baseline': 1,
        '6-month post-baseline': 2,
        '12-month post-baseline': 3
    }

    windows = file_map.get('assessment_window')
    if not windows:
        raise ValueError("file_map does not any contain 'assessment_window'.")
    if not isinstance(windows, list):
        windows = [windows]

    target_values = [window_map[value] for value in windows]
    target_codes = [code_map[value] for value in windows]

    filtered_files = {}

    for filename in os.listdir(source_path):
        if not filename.lower().endswith('.csv'):
            continue

        file_path = os.path.join(source_path, filename)
        try:
            df = pd.read_csv(file_path, sep=';')

            if 'visit_name' in df.columns:
                mask = df['visit_name'].str.contains(target_values, case=False, na=False)
            elif 'SiteCode' in df.columns:
                mask = df['VisitCode'].isin(target_codes)
            else:
                print(f"{filename}: skipped (no 'visit_name' or 'VisitCode' column found)")
                continue

            filtered_df = df[mask]

            filtered_df.to_csv(os.path.join(source_path, f"{os.path.splitext(filename)[0]}_filtered.csv"), index=False, sep=';')
            filtered_files[filename] = filtered_df
            print(f"Saved {filename} ({len(filtered_df)} rows)")

        except Exception as e:
            print(f"Error processing {filename}: {e}")
    return filtered_files


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
            df.to_csv(out_path_headers, index=False, sep=";")

    return processed_dataframes, filenames
