import csv
import os
import yaml
import pandas as pd

source = ""


def rename_columns(directory):
    print(f"Renaming columns...")
    for file in os.listdir(directory):
        if file.endswith(".csv"):
            current_path = os.path.join(directory, file)
            df = pd.read_csv(current_path, sep=";")

            # REDCap data for drops record ID 31
            if file.startswith("documentation"):
                df = df.rename(columns={
                    df.columns[0]: "participant_identifier",
                    df.columns[1]: "event_name",
                    df.columns[2]: "Has the first contact already been established?",
                    df.columns[3]: "On which unit is the candidate?",
                    df.columns[4]: "What are the reasons why the participant has not yet been approached for the study? (choice=Feeling of the practitioner/study staff that patient is currently too severely ill)",
                    df.columns[5]: "What are the reasons why the participant has not yet been approached for the study? (choice=assumed language barrier)",
                    df.columns[6]: "What are the reasons why the participant has not yet been approached for the study? (choice=An attempt was made to establish contact, but the patient could not be found on the unit. In this case: leave a flyer for the patient with ward staff)",
                    df.columns[7]: "What are the reasons why the participant has not yet been approached for the study? (choice=Patient has already refused study participation previously)",
                    df.columns[8]: "What are the reasons why the participant has not yet been approached for the study? (choice=other)",
                    df.columns[9]: "Optional: Here is place to enter another possible reason why no contact could be established yet:",
                    df.columns[10]: "After the first contact: What were the reasons that the candidate did not participate in the educational interview for the study? (choice=too severe illness at the moment)",
                    df.columns[11]: "After the first contact: What were the reasons that the candidate did not participate in the educational interview for the study? (choice=not able to speak local language properly)",
                    df.columns[12]: "After the first contact: What were the reasons that the candidate did not participate in the educational interview for the study? (choice=treating clinician does not want to participate in IMMERSE)",
                    df.columns[13]: "After the first contact: What were the reasons that the candidate did not participate in the educational interview for the study? (choice=declined to participate without indicating reasons)",
                    df.columns[14]: "After the first contact: What were the reasons that the candidate did not participate in the educational interview for the study? (choice=other:)",
                    df.columns[15]: "Optional: Here is space to enter another possible reason for declining participation in the study:",
                    df.columns[16]: "Did an educational interview to inform about the study take place?",
                    df.columns[17]: "If an educational interview has not yet taken place: What were the reasons why the educational interview was not carried out? (choice=Participant has not appeared) ",
                    df.columns[18]: "If an educational interview has not yet taken place: What were the reasons why the educational interview was not carried out? (choice=Participant has changed her mind & canceled)",
                    df.columns[19]: "If an educational interview has not yet taken place: What were the reasons why the educational interview was not carried out? (choice=Informed consent sheet was not available during educational interview)",
                    df.columns[20]: "If an educational interview has not yet taken place: What were the reasons why the educational interview was not carried out? (choice=Other)",
                    df.columns[21]: "Optional: Here is space to specify further reasons:",
                })
            else:
                df = df.rename(columns={
                    df.columns[0]: "participant_identifier",
                    df.columns[1]: "event_name",
                    df.columns[2]: "consent",
                    df.columns[3]: "condition",
                    df.columns[4]: "t1_dropout",
                    df.columns[5]: "t2_dropout",
                    df.columns[6]: "t3_dropout"
                })

            filename = os.path.basename(current_path)
            print(f"Renaming {filename}... ")
            new_filepath = os.path.join(directory, f"redcap_{filename}")
            df.to_csv(new_filepath, sep=";", index=False)


def filter_and_rename_values_in_df(directory):
    print(f"Filtering and renaming values...")
    redcap_values = [
        "In contact  (Arm 2: In contact)",
        "In contact (Arm 2: In contact)",
        "First contact (Arm 2: In contact)",
        "Baseline (Arm 1: Included)",
        "T1 (Arm 1: Included)",
        "T2 (Arm 1: Included)",
        "T3 (Arm 1: Included)"
    ]
    for filename in os.listdir(directory):
        if filename.startswith("redcap") and filename.endswith(".csv"):
            df = pd.read_csv(os.path.join(directory, filename), sep=";")
            df_filtered = df[df["event_name"].isin(redcap_values)]
            df_filtered["event_name"] = df_filtered["event_name"].replace(
                {
                    "In contact  (Arm 2: In contact)": "In contact",
                    "First contact (Arm 2: In contact)": "In contact",
                    "In contact (Arm 2: In contact)": "In contact",
                    "Baseline (Arm 1: Included)": "Baseline",
                    "T1 (Arm 1: Included)": "T1",
                    "T2 (Arm 1: Included)": "T2",
                    "T3 (Arm 1: Included)": "T3"
                }
            )
            print(f"Filtering and renaming values from {filename}... ")
            new_filepath = os.path.join(directory, f"filtered_{filename}")
            df_filtered.to_csv(new_filepath, sep=";", index=False)


def detect_separator(filepath):
    with open(filepath, 'r',  encoding='utf-8', errors='ignore') as f:
        first_line = f.readline()

    for delimiter in [',', ';']:
        if delimiter in first_line:
            return delimiter


def rename_files(filepath, word_to_replace, replacement):
    for file in os.listdir(filepath):
        if not file.startswith("ITEM") and file.endswith(".csv"):
            if word_to_replace in file:
                new_name = file.replace(file, f"{replacement}_{file}")
                print(new_name)
                os.rename(os.path.join(filepath, file), os.path.join(filepath, new_name))
            else:
                print(f"{word_to_replace} not found in {file}")


def load_config_file(directory, file):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, "config.yaml")
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
        if file:
            return config[directory][file]
        else:
            return config[directory]


def write_config_file(filepath, file, key="data_requirements"):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, "config.yaml")

    def find_file_key(data, search_key,  target_directory):
        if isinstance(data, dict):
            for k, v in data.items():
                # Only search inside the desired key
                if k == search_key and isinstance(v, dict):
                    for sub_key, sub_value in v.items():
                        if isinstance(sub_value, str) and (os.path.isdir(sub_value) or sub_value == target_directory):
                            return sub_key
                elif isinstance(v, dict):
                    result = find_file_key(v, search_key, target_directory)
                    if result:
                        return result
        return None

    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
            key_file = find_file_key(config, key,  filepath)
    else:
        config = {}

    full_file_path = os.path.join(filepath, file)
    config[key][key_file] = full_file_path

    with open(config_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(config, f, default_flow_style=False)

    return full_file_path


def merge_files(source_path, new_filename):
    all_dataframes = []
    for filename in os.listdir(source_path):
        if filename.endswith('.xlsx') or filename.endswith('.csv'):
            if filename.startswith('Logins'):
                filepath = os.path.join(source_path, filename)
                df = pd.read_csv(filepath, sep=';', encoding='cp1250') if filepath.endswith('.csv') else pd.read_excel(filepath)
                all_dataframes.append(df)

    merged_dataframes = pd.concat(all_dataframes, ignore_index=True).drop_duplicates()
    merged_dataframes.to_excel(os.path.join(source_path, new_filename), index=False)
    print("Merging done.")


def get_unique_values_from_columns(file_path, column1, column2, new_filename):
    df = pd.read_excel(file_path)
    df[column1] = df[column1].astype(str).str.strip()
    df[column2] = df[column2].astype(str).str.strip()

    df_unique = df[[column1, column2]].drop_duplicates().reset_index(drop=True)

    output_path = os.path.join(source, new_filename)
    df_unique.to_excel(output_path, index=False)

# merge_files(source_path="", new_filename="")
# get_unique_values_from_columns(file_path="",
#                                column1="firstName",
#                                column2="lastName",
#                                new_filename='unique_values.xlsx'
#                                )
