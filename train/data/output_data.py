import warnings

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")


async def get_submitter_reliability(
    df,
    X_cols,
    y_cols,
    model_dict,
    submitter_name,
):
    submitter_info = df[["submitter_person_id"] + X_cols].drop_duplicates()
    submitter_info = submitter_info.drop_duplicates()
    submitter_records = submitter_info.merge(
        right=submitter_name,
        left_on="submitter_person_id",
        right_on="id",
        how="inner",
    )
    submitter_records[
        "n_support"
    ] = submitter_info.submitter_person_id.value_counts()[
        submitter_records["submitter_person_id"]
    ].to_list()
    submitter_records = submitter_records.drop_duplicates(
        subset="submitter_person_id",
    )
    anal_df = submitter_records[
        ["submitter_name", "submitter_person_id", "n_support"]
    ]
    reliability_cols = []
    for col in y_cols:
        clf = model_dict[col]
        prob = clf.predict_proba(submitter_records[X_cols])[:, 1]
        reliability_col = col.replace("label", "reliability")
        anal_df[reliability_col] = prob
        reliability_cols.append(reliability_col)

    cols_to_average = [
        "tenant_name_reliability",
        "space_type_id_reliability",
        "transaction_size_reliability",
        "starting_rent_reliability",
        "execution_date_reliability",
        "commencement_date_reliability",
        "lease_term_reliability",
        "expiration_date_reliability",
    ]

    anal_df["general_reliability"] = anal_df[cols_to_average].mean(axis=1)
    anal_df = anal_df.sort_values(
        by=["general_reliability", "n_support"],
        ascending=False,
    ).reset_index(drop=True)
    return anal_df, submitter_info


def get_version_reliability(
    data,
    attributes,
    submitter_info,
    y_labels,
    model_dict,
):

    val_columns = ["type", "master_comp_data_id", "version_comp_data_id"]
    for att in attributes:
        val_columns.append(att)
        val_columns.append(f"{att}_prob")
    val_df = pd.DataFrame(columns=val_columns)
    val_df["master_comp_data_id"] = np.NaN
    val_df["version_comp_data_id"] = np.NaN

    comp_data_master_ids = list(set(data.comp_data_id_master))
    for comp_data_master_id in comp_data_master_ids:
        master_df = data[data.comp_data_id_master == comp_data_master_id]

        for version in set(master_df.comp_data_id_version):
            version_df = master_df[master_df.comp_data_id_version == version]
            version_dict = {}
            version_dict["type"] = "Version"
            version_dict["master_comp_data_id"] = comp_data_master_id
            version_dict["version_comp_data_id"] = version
            submitter_person_id = version_df.iloc[0]["submitter_person_id"]
            X = (
                submitter_info[
                    submitter_info.submitter_person_id == submitter_person_id
                ]
                .head(1)
                .drop(["submitter_person_id"], axis=1)
            )
            for i in range(0, len(attributes)):
                model_name = y_labels[i]
                clf = model_dict[model_name]
                probs = clf.predict_proba(X)
                version_dict[attributes[i]] = version_df.iloc[0][
                    f"{attributes[i]}_version"
                ]
                prob = probs[0, 1]
                version_dict[f"{attributes[i]}_prob"] = prob
            val_df = val_df.append(version_dict, ignore_index=True)

    return val_df
