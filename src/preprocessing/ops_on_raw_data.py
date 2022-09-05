import os
import pandas as pd
import logging


def check_directory_absence(name, path):
    directories = os.listdir(path)
    if not name in directories:
        return True
    else:
        return False


def ops_on_corona():
    group_by_month_corona()
    extract_only_en()


def group_by_month_corona():
    # Read raw data from files with '-**-' the name where ** can take the values 01, 02, 03 for January through March
    # Output 4 files one for each month and one containing all of the data.
    # TODO there are a large number of changes to this function that have not had significant testing.
    #   Without the original raw data they cannot be sufficiently tested but this function is not needed for now.

    # Set path variables
    path = os.path.join(os.getcwd(), 'data', 'corona_virus')
    raw_path = os.path.join(path, 'raw_data')
    group_path = os.path.join(path, 'group_data')

    # If the group_data folder does not exist create it.
    if not os.path.exists(group_path):
        os.mkdir('group_data')

    # os.chdir(os.path.join(path, 'raw_data'))
    raw_data = [element for element in os.listdir(raw_path) if os.path.isfile(os.path.join(raw_path, element))]

    # Determine the date of the data by text in the file name
    # TODO make this irrespective of file name
    january_data = [dataset for dataset in raw_data if '-01-' in dataset]
    february_data = [dataset for dataset in raw_data if '-02-' in dataset]
    march_data = [dataset for dataset in raw_data if '-03-' in dataset]

    all_data = []
    for file in raw_data:
        all_data.append(get_data_from_files([os.path.join(raw_path, file)]))

    df = pd.concat(all_data, axis=0, ignore_index=True)

    january_data = get_data_from_files(january_data)
    february_data = get_data_from_files(february_data)
    march_data = get_data_from_files(march_data)

    os.chdir(os.path.join(path, 'group_data'))

    january_data.to_csv('January_dataset.csv', encoding='utf-8', index=False)
    february_data.to_csv('February_dataset.csv', encoding='utf-8', index=False)
    march_data.to_csv('March_dataset.csv', encoding='utf-8', index=False)
    all_data.to_csv('All_data.csv', encoding='utf-8', index=False)


def get_data_from_files(files):
    rows = []
    for filename in files:
        df = pd.read_csv(files[0], usecols=[
            'username', 'favorites', 'retweets', 'language', '@mentions', 'geo', 'text_con_hashtag'
        ])
        rows.append(df)
        logging.info("Read %s rows from %s", df.shape[0], filename)

    return rows


def extract_only_en():
    # TODO this function does not work currently it is not needed until we have the original raw data.
    path = os.path.join(os.getcwd(), 'data', 'corona_virus')
    if check_directory_absence('final_data', path):
        os.mkdir('final_data')
        os.chdir(os.path.join(path, 'group_data'))

        df = pd.read_csv('All_data.csv')
        filt = df['language'] == 'en'
        df_only_en = df[filt]
        df_only_en.drop(columns=['language'], inplace=True)
        df_only_en['@mentions'] = df_only_en['@mentions'].fillna('self')
        df_only_en.dropna()
        print('ONLY EN TWITTER FINAL SHAPE')
        print(df_only_en.shape)
        print()

        os.chdir(os.path.join(path, 'final_data'))
        df_only_en.to_csv('Final_data.csv', encoding='utf-8', index=False)


def ops_on_vac():
    refine_data()


def refine_data():
    starting_path = os.getcwd()
    path = os.path.join(starting_path, 'data/vax_no_vax')
    if check_directory_absence('final_data', path):
        os.mkdir('final_data')
        os.chdir(os.path.join(path, 'raw_data'))
        df = pd.read_csv('./full_data.csv', usecols=['date', 'username', 'replies_count', 'retweets_count',
                                                     'likes_count', 'hashtags', 'mentions', 'tweet'])
        os.chdir(os.path.join(path, 'final_data'))
        df['mentions'].replace('[]', "['self']", inplace=True)
        df['hashtags'].replace('[]', "['noOne']", inplace=True)
        df.to_csv('Final_data.csv', encoding='utf-8', index=False)

        print('VACCINATION DATA FINAL SHAPE')
        print(df.shape)

    os.chdir(starting_path)
