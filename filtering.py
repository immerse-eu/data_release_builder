import os
import pandas as pd


def assessment_window_filtering(assessment_list, source_path):
    print(f"\nFiltering by {assessment_list} assessment window ...")

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

    windows = assessment_list
    if not windows:
        raise ValueError("'assessment_list' does not contain any defined values.")
    if not isinstance(windows, list):
        windows = [windows]

    target_values = [window_map[value] for value in windows if value in window_map]
    target_codes = [code_map[value] for value in windows if value in code_map]
    pattern_values = '|'.join(target_values)

    target_codes_str = [str(code) for code in target_codes]

    filtered_files = {}

    for filename in os.listdir(source_path):
        if not filename.lower().endswith('.csv'):
            continue

        file_path = os.path.join(source_path, filename)
        try:
            df = pd.read_csv(file_path, sep=';')

            if 'VisitCode' in df.columns and target_codes_str:
                df['VisitCode'] = df['VisitCode'].astype(str)
                mask = df['VisitCode'].isin(target_codes_str)

            elif 'visit_name' in df.columns and target_values:
                mask = df['visit_name'].str.contains(pattern_values, case=False, na=False)

            else:
                print(f"{filename}: skipped (no 'visit_name' or 'VisitCode' column found)")
                continue

            filtered_df = df[mask]
            filename = f"{os.path.splitext(filename)[0]}_filtered.csv"

            filtered_df.to_csv(os.path.join(source_path, filename), index=False, sep=';')
            filtered_files[filename] = filtered_df
            print(f"Saved {filename} ({len(filtered_df)} rows)")

        except Exception as e:
            print(f"Error processing {filename}: {e}")

    return filtered_files


def filtering_excluded_ids(baseline_ids_path, source_path):
    print("\nRemoving excluded ids from baselines...")
    filenames = []
    processed_dataframes = []

    ids_to_exclude = set(pd.read_excel(baseline_ids_path, header=None).iloc[:, 0].astype(str))
    print(f"Excluded {len(ids_to_exclude)}")

    for filename in os.listdir(source_path):
        if filename.endswith('.csv') and filename.endswith('_filtered.csv'):
            file_path = os.path.join(source_path, filename)
            df = pd.read_csv(file_path, sep=';')

            if 'participant_identifier' not in df.columns:
                print(f"participant_identifier not found in dataframe {filename}")
                continue

            df['participant_identifier'] = df['participant_identifier'].str.strip()
            df = df[~df['participant_identifier'].isin(ids_to_exclude)]

            processed_dataframes.append(df)
            filenames.append(filename)
            filename = f"{filename.replace("_filtered", "_exclusion_filter")}"

            out_path_headers = os.path.join(source_path, filename)
            df.to_csv(out_path_headers, index=False, sep=";")
            print(f"Saved {filename} ({len(df)} rows)")

    return processed_dataframes, filenames


def filtering_interesting_ids(baseline_ids_path, source_path):
    print("\nFiltering additional interesting_ids...")
    filenames = []
    processed_dataframes = []

    ids_to_include = set(pd.read_excel(baseline_ids_path, header=None).iloc[:, 0].astype(str))
    print(f"Excluded {len(ids_to_include)}")

    for filename in os.listdir(source_path):
        if filename.endswith('.csv') and filename.endswith('_exclusion_filter.csv'):
            file_path = os.path.join(source_path, filename)
            df = pd.read_csv(file_path, sep=';')

            if 'participant_identifier' not in df.columns:
                print(f"participant_identifier not found in dataframe {filename}")
                continue

            df['participant_identifier'] = df['participant_identifier'].str.strip()
            df = df[df['participant_identifier'].isin(ids_to_include)]

            processed_dataframes.append(df)
            filenames.append(filename)
            filename = f"ITEM_{filename.replace("_exclusion_filter", "")}"

            out_path_headers = os.path.join(source_path, filename)
            df.to_csv(out_path_headers, index=False, sep=";")
            print(f"Saved {filename} ({len(df)} rows)")

    return processed_dataframes, filenames
