import os
import pandas as pd

pseudo_dict = {}


def detect_separator(filepath):
    with open(filepath, 'r',  encoding='utf-8', errors='ignore') as f:
        first_line = f.readline()

    for delimiter in [',', ';']:
        if delimiter in first_line:
            return delimiter


def get_pseudo_dict(pseudo_path):
    pseudo_file_path = None

    for file_name in os.listdir(pseudo_path):
        if "gPAS" in file_name and file_name.endswith(".csv"):
            pseudo_file_path = os.path.join(pseudo_path, file_name)

    if pseudo_file_path:
        sep = detect_separator(pseudo_file_path)
        try:
            df = pd.read_csv(pseudo_file_path, sep=sep)
            pseudonym_map = dict(zip(df.iloc[:, 0].astype(str), df.iloc[:, 1].astype(str)))
            return pseudonym_map

        except Exception as e:
            return print(f"Error reading file {pseudo_file_path}:", e)


def pseudo_ids_process(pseudo_map_path, original_output_path):
    study_file_paths = []

    parent_directory = os.path.dirname(original_output_path)
    pseudo_output_path = os.path.join(parent_directory, 'pseudo')

    if not os.path.exists(pseudo_output_path):
        os.makedirs(pseudo_output_path)

    pseudo_map = get_pseudo_dict(pseudo_map_path)
    print("Mapping obtained:", pseudo_map)

    for file_name in os.listdir(original_output_path):
        if file_name.endswith(".csv") and not file_name.endswith("_no_headers.csv"):
            study_file_paths.append(os.path.join(original_output_path, file_name))

    unmapped_report = {}  # {filename: [ids]}

    for study_file in study_file_paths:
        try:
            sep = detect_separator(study_file)
            df = pd.read_csv(study_file, sep=sep)

            mapped_ids = df['participant_identifier'].map(pseudo_map)

            missing_mask = mapped_ids.isna()
            missing_ids = (
                df.loc[missing_mask, 'participant_identifier']
                .dropna()
                .unique()
                .tolist()
            )

            if missing_ids:
                unmapped_report[os.path.basename(study_file)] = missing_ids

            df_cleaned = df.loc[~missing_mask].copy()
            df_cleaned['participant_identifier'] = mapped_ids[~missing_mask]

            output_file_path = os.path.join(pseudo_output_path, os.path.basename(study_file))
            df_cleaned.to_csv(output_file_path, sep=sep, index=False)

            return pseudo_output_path

        except Exception as e:
            print(f"Error reading file {study_file}: {e}")

    for filename, ids in unmapped_report.items():
        print(f"[UNMAPPED IDS] {filename}: {ids}")