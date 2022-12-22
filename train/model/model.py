from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, f1_score
from sklearn.model_selection import train_test_split

from train.data.database_io import get_reliable_data_by_attribute


def get_column_names(attributes):
    correct = []
    filled = []
    label = []
    for att in attributes:
        correct.append(f"{att}_correct")
        filled.append(f"{att}_filled")
        label.append(f"{att}_label")
    return correct, filled, label


def get_split_columns(columns):
    X_cols = []
    y_cols = []
    for col in columns:
        if col.endswith("count") or col.endswith("rate"):
            X_cols.append(col)
        elif col.endswith("label"):
            y_cols.append(col)

    return X_cols, y_cols


def train_multioutput_classifiers(df, X_cols, y_cols):

    model_dict = {}
    df_reliable_attributes = get_reliable_data_by_attribute()
    for col in y_cols:

        # Remove null attributes
        attribute = col.replace("_label", "")
        ids_to_keep = df_reliable_attributes[
            df_reliable_attributes[f"{attribute}_count"] >= 3
        ]["comp_version_id"].tolist()

        temp = df[(df["id"].isin(ids_to_keep)) & (df[col] != -1)]
        X = temp[X_cols]
        y = temp[col]
        x_train, x_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=0.2,
            random_state=10,
        )
        class_weight = "balanced"
        clf = RandomForestClassifier(
            class_weight=class_weight,
            random_state=1,
            n_jobs=-1,
        )
        clf.fit(x_train, y_train)
        test_preds = clf.predict(x_test)
        acc = accuracy_score(y_test, test_preds)
        f1 = f1_score(y_test, test_preds)

        print(attribute)
        print(f"Training Data Size: {len(x_train)}")
        print(f"{col} - Accuracy : {acc}")
        print(f"{col} - F1 : {f1}")
        print(y.value_counts())
        print("----------------------------------")

        model_dict[col] = clf

    return model_dict
