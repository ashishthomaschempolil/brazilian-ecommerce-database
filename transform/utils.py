import pandas as pd
import numpy as np
import unidecode
import difflib

def convert_to_unaccented_string(string: str) -> str:
    """Converts a string to an unaccented string.
    Eg: "café" -> "cafe"

    :param str string: The string to convert
    :return str: The converted string
    """
    return unidecode.unidecode(string)


def preprocess_city_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Preprocesses the city column of the dataframe.

    :param pd.DataFrame df: The dataframe
    :param str column: The column to preprocess
    :return pd.DataFrame: The preprocessed dataframe
    """
    df[column] = df[column].apply(
        convert_to_unaccented_string
    )  # convert to unaccented string
    df[column] = df[column].str.lower()  # convert to lowercase
    df[column] = df[column].str.replace("_", " ")  # replace _ with space
    df[column] = (
        df[column].str.split("/").str[0].str.strip()
    )  # remove everything after the first /
    df[column] = (
        df[column].str.split("\\").str[0].str.strip()
    )  # remove everything after the first \

    return df


def set_asstr(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Sets the column as a string type.

    :param pd.DataFrame df: The dataframe
    :param str column: The column to set as string
    :return pd.DataFrame: The dataframe with the column set as string
    """
    df[column] = df[column].astype(str)
    return df


def preprocess_geolocation(df: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses the geolocation dataframe.

    :param pd.DataFrame df: The geolocation dataframe
    :param list cities: The list of standard city names
    :return pd.DataFrame: The preprocessed dataframe
    """
    df = preprocess_city_column(df, "geolocation_city")
    df.drop(
        columns=["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"],
        inplace=True,
    )
    df.drop_duplicates(inplace=True, keep="first")
    df["geolocation_city"] = df["geolocation_city"].astype(str)
    df["geolocation_state"] = df["geolocation_state"].astype(str)
    return df


def append_rows_to_geolocation(
    df: pd.DataFrame, columns: list, geolocation_df: pd.DataFrame
) -> pd.DataFrame:
    """Appends (city, state) values that are present in df but not in geolocation_df to geolocation_df.

    :param pd.DataFrame df: The dataframe containing the (city, state) values to be appended
    :param list columns: The list of column names containing the (city, state) values respectively
    :param pd.DataFrame geolocation_df: The geolocation dataframe
    :return pd.DataFrame: The merged geolocation dataframe
    """
    geolocation_df["city_state"] = (
        geolocation_df["geolocation_city"] + "_" + geolocation_df["geolocation_state"]
    )

    df = df.copy()
    # now for df
    df["city_state"] = df[columns[0]] + "_" + df[columns[1]]
    df.drop_duplicates(subset=["city_state"], inplace=True, keep="first")

    # get the rows that are present in df but not in geolocation_df
    df = df[~df["city_state"].isin(geolocation_df["city_state"])]

    df.rename(
        columns={columns[0]: "geolocation_city", columns[1]: "geolocation_state"},
        inplace=True,
    )

    # append the rows to geolocation_df
    geolocation_df = pd.concat([
        geolocation_df, 
        df.loc[:, ["geolocation_city", "geolocation_state", "city_state"]]
    ], ignore_index=True)


    # drop city_state column
    geolocation_df.drop(columns=["city_state"], inplace=True)
    df.drop(columns=["city_state"], inplace=True)

    return geolocation_df


def make_common_city_values(
    df: pd.DataFrame, column: str, cities: list
) -> pd.DataFrame:
    """Makes city values standard across different dataframes

    :param pd.DataFrame df: The dataframe
    :param str column: The city column
    :param list cities: The list of standard city names
    :return pd.DataFrame: The dataframe with standard city names
    """
    for index, row in df.iterrows():
        if row[column] not in cities:
            closest_match = difflib.get_close_matches(
                row[column], cities, n=2, cutoff=0.7
            )
            if (
                closest_match
            ):  # if there is a match, assign it otherwise leave it as it is
                df.loc[index, column] = closest_match[0]
    return df