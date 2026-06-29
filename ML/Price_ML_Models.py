import sqlite3
import warnings
from math import prod
from datetime import datetime
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from category_encoders import TargetEncoder
from lightgbm import LGBMClassifier, LGBMRegressor
from sklearn.compose import ColumnTransformer, TransformedTargetRegressor
from sklearn.ensemble import RandomForestRegressor, VotingClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import HuberRegressor, LogisticRegression, Ridge
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    mean_absolute_error,
    mean_absolute_percentage_error,
    mean_squared_error,
    r2_score,
    roc_auc_score,
)
from sklearn.model_selection import RandomizedSearchCV, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

warnings.filterwarnings("ignore")

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "CAR_DATA_OUTPUT" / "CAR_DATA_CLEANED.db"
ABSA_DB_PATH = BASE_DIR / "CAR_DATA_OUTPUT" / "CAR_YOUTUBE_COMMENTS.db"
OUTPUT_DIR = BASE_DIR / "MODELS_OUTPUT"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

HIGH_VALUE_THRESHOLD = 150_000
RANDOM_STATE = 42


def load_and_split_data():
    """Load merged car and ABSA data, then split by the high-value routing target."""
    print(f"Loading data from {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("ATTACH DATABASE ? AS absa", (str(ABSA_DB_PATH),))

    cols_to_drop = [
        "vin",
        "year",
        "details",
        "title",
        "date",
        "location",
        "currentBid",
        "bids",
        "vehicleTitleDesc",
        "img",
        "nhtsa_Trim2",
        "nhtsa_safety_ratings_count",
        "nhtsa_overall_rating",
        "nhtsa_front_crash_rating",
        "nhtsa_recall_components",
        "nhtsa_latest_recall_date",
        "nhtsa_complaint_injuries",
        "nhtsa_complain_deaths",
        "nhtsa_complaint_crash_related",
        "nhtsa_complaint_fire_related",
        "nhtsa_common_complaint_areas",
        "nhtsa_ModelID",
        "nhtsa_MakeID",
        "nhtsa_BasePrice",
        "loaddate",
        "nhtsa_AdditionalErrorText",
        "nhtsa_ManufacturerId",
        "locationCode",
        "nhtsa_OtherEngineInfo",
        "nhtsa_DisplacementCC",
        "nhtsa_DisplacementCI",
        "countryCode",
        "nhtsa_complaint_deaths",
    ]

    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info('listings')")
    listings_cols = [row[1] for row in cursor.fetchall()]
    cursor.execute("PRAGMA table_info('nhtsa_enrichment')")
    nhtsa_cols = [row[1] for row in cursor.fetchall() if row[1] != "vin"]

    selected_cols = [c for c in listings_cols + nhtsa_cols if c not in cols_to_drop]
    selected_listing_cols = [f"l.{c}" for c in listings_cols if c in selected_cols]
    selected_nhtsa_cols = [f"n.{c}" for c in nhtsa_cols if c in selected_cols]
    vehicle_entity_expr = (
        "CAST(n.nhtsa_ModelYear AS TEXT) || ' ' || n.nhtsa_Make || ' ' || n.nhtsa_Model"
    )
    absa_cols = [
        "vsi.Reliability_Index",
        "vsi.General_Enthusiast_Score",
        "vsi.Sentiment_Volatility_StdDev",
        "vsi.Sentiment_Trend_Slope",
        "vsi.Confidence_Level",
    ]
    query_cols = selected_listing_cols + selected_nhtsa_cols + [
        f"{vehicle_entity_expr} AS Vehicle_Entity",
        *absa_cols,
    ]

    query = f"""
    SELECT {', '.join(query_cols)}
    FROM listings AS l
    JOIN nhtsa_enrichment AS n USING(vin)
    LEFT JOIN absa.Vehicle_Sentiment_Index AS vsi
        ON {vehicle_entity_expr} = vsi.Vehicle_Entity
    """

    df = pd.read_sql_query(query, conn)
    conn.close()

    float_cols = df.select_dtypes(include=["float64"]).columns
    int_cols = df.select_dtypes(include=["int64"]).columns
    df[float_cols] = df[float_cols].astype("float32")
    df[int_cols] = df[int_cols].astype("int32")

    year_col = "nhtsa_ModelYear" if "nhtsa_ModelYear" in df.columns else "Year"
    target_col = "price" if "price" in df.columns else "Price"

    df["vehicle_age"] = datetime.now().year - pd.to_numeric(df[year_col], errors="coerce")
    df["vehicle_age"] = df["vehicle_age"].astype("float32")
    df["is_high_value"] = (df[target_col] >= HIGH_VALUE_THRESHOLD).astype("int32")

    train_df, test_df = train_test_split(
        df,
        test_size=0.2,
        random_state=RANDOM_STATE,
        stratify=df["is_high_value"],
    )
    train_df = train_df.reset_index(drop=True)
    test_df = test_df.reset_index(drop=True)

    print(f"Train size: {train_df.shape[0]:,} | Test size: {test_df.shape[0]:,}")
    print(
        "High-value train/test rates: "
        f"{train_df['is_high_value'].mean():.4f} / {test_df['is_high_value'].mean():.4f}"
    )

    feature_drop_cols = [target_col, "is_high_value", "Vehicle_Entity"]
    X_train = train_df.drop(columns=feature_drop_cols, errors="ignore")
    y_train = train_df[target_col]
    X_test = test_df.drop(columns=feature_drop_cols, errors="ignore")
    y_test = test_df[target_col]

    return X_train, y_train, X_test, y_test


def build_pipelines(X_train):
    """Build tree and linear preprocessors with bounded one-hot expansion."""
    numeric_features = X_train.select_dtypes(include=[np.number]).columns.tolist()
    categorical_features = X_train.select_dtypes(exclude=[np.number]).columns.tolist()

    low_cardinality_threshold = 50
    target_encoded_name_tokens = ("make", "model", "trim")
    forced_target_cols = [
        c for c in categorical_features if any(token in c.lower() for token in target_encoded_name_tokens)
    ]
    low_card_cols = [
        c
        for c in categorical_features
        if c not in forced_target_cols and X_train[c].nunique(dropna=True) <= low_cardinality_threshold
    ]
    high_card_cols = [
        c
        for c in categorical_features
        if c in forced_target_cols or X_train[c].nunique(dropna=True) > low_cardinality_threshold
    ]

    numeric_tree = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
        ]
    )
    numeric_linear = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
        ]
    )
    categorical_low = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="constant", fill_value="UNKNOWN")),
            ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=True)),
        ]
    )
    categorical_high = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="constant", fill_value="UNKNOWN")),
            ("target", TargetEncoder()),
        ]
    )

    preprocessor_tree = ColumnTransformer(
        transformers=[
            ("num", numeric_tree, numeric_features),
            ("cat_low", categorical_low, low_card_cols),
            ("cat_high", categorical_high, high_card_cols),
        ],
        remainder="drop",
    )
    linear_feature_encoder = ColumnTransformer(
        transformers=[
            ("num", numeric_linear, numeric_features),
            ("cat_low", categorical_low, low_card_cols),
            ("cat_high", categorical_high, high_card_cols),
        ],
        remainder="drop",
    )
    preprocessor_linear = Pipeline(
        steps=[
            ("features", linear_feature_encoder),
            ("scaler", StandardScaler(with_mean=False)),
        ]
    )

    print(f"Numeric features: {len(numeric_features)}")
    print(f"Low-cardinality categorical features: {len(low_card_cols)}")
    print(f"High-cardinality target-encoded features: {len(high_card_cols)}")

    return preprocessor_tree, preprocessor_linear, numeric_features, low_card_cols, high_card_cols


def sample_training_data(X_train, y_train, sample_size, stratify=None):
    """Return a reproducible tuning subset without changing the full-training fit."""
    if X_train.shape[0] <= sample_size:
        return X_train, y_train

    _, X_sample, _, y_sample = train_test_split(
        X_train,
        y_train,
        test_size=sample_size,
        random_state=RANDOM_STATE,
        stratify=stratify,
    )
    return X_sample, y_sample


def evaluate_regressor(name, model, X_test, y_test):
    preds = model.predict(X_test)
    rmse = np.sqrt(mean_squared_error(y_test, preds))
    mae = mean_absolute_error(y_test, preds)
    mape = mean_absolute_percentage_error(y_test, preds)
    r2 = r2_score(y_test, preds)

    print(f"--- {name} Results ---")
    print(f"RMSE:  {rmse:.4f}")
    print(f"MAE:   {mae:.4f}")
    print(f"MAPE:  {mape:.4f} ({mape * 100:.2f}%)")
    print(f"R2:    {r2:.4f}\n")
    return rmse, mae, mape, r2


def tune_and_train_classifier(pipeline, X_train, y_train):
    """Tune voting weights on a subset, then fit the router on all training rows."""
    sample_size = min(250_000, X_train.shape[0])
    X_tune, y_tune = sample_training_data(
        X_train,
        y_train,
        sample_size,
        stratify=y_train if y_train.nunique() > 1 else None,
    )

    print(f"\nTuning high-value router on {X_tune.shape[0]:,} rows...")
    search = RandomizedSearchCV(
        pipeline,
        param_distributions={
            "weights": [
                [0.9, 0.1],
                [0.8, 0.2],
                [0.7, 0.3],
                [0.6, 0.4],
                [0.5, 0.5],
                [0.4, 0.6],
            ]
        },
        n_iter=6,
        cv=3,
        scoring="roc_auc",
        n_jobs=-1,
        random_state=RANDOM_STATE,
        verbose=1,
    )
    search.fit(X_tune, y_tune)
    print(f"Best classifier parameters: {search.best_params_}")

    best_model = search.best_estimator_
    print(f"Training high-value router on full dataset ({X_train.shape[0]:,} rows)...")
    best_model.fit(X_train, y_train)
    return best_model


def tune_and_train_regressor(pipeline, param_grid, X_train, y_train, model_name, sample_size=100_000):
    """Tune a regressor on a subset, then fit the best estimator on the full routed segment."""
    if X_train.empty:
        raise ValueError(f"{model_name} received an empty training segment.")

    sample_size = min(sample_size, X_train.shape[0])
    X_tune, y_tune = sample_training_data(X_train, y_train, sample_size)

    print(f"\nTuning {model_name} on {X_tune.shape[0]:,} rows...")
    total_param_combinations = prod(len(values) for values in param_grid.values()) if param_grid else 1
    search = RandomizedSearchCV(
        pipeline,
        param_distributions=param_grid,
        n_iter=min(8, total_param_combinations),
        cv=3,
        scoring="neg_root_mean_squared_error",
        n_jobs=-1,
        random_state=RANDOM_STATE,
        verbose=1,
    )
    search.fit(X_tune, y_tune)
    print(f"Best parameters for {model_name}: {search.best_params_}")

    best_model = search.best_estimator_
    print(f"Training {model_name} on full segment ({X_train.shape[0]:,} rows)...")
    best_model.fit(X_train, y_train)
    return best_model


def evaluate_classifier(classifier, X_test, y_test):
    y_route = (y_test >= HIGH_VALUE_THRESHOLD).astype(int)
    predicted_labels = classifier.predict(X_test)
    predicted_probs = classifier.predict_proba(X_test)[:, 1]

    print("--- Stage 1 Classifier Results ---")
    print(f"Accuracy: {accuracy_score(y_route, predicted_labels):.4f}")
    print(f"ROC AUC:  {roc_auc_score(y_route, predicted_probs):.4f}")
    print(classification_report(y_route, predicted_labels, digits=4))


def evaluate_full_pipeline(classifier, regressor_everyday, regressor_exotic, X_test, y_test):
    """Route test rows through the classifier, then score the combined price predictions."""
    predicted_labels = classifier.predict(X_test)
    predictions = np.empty(X_test.shape[0], dtype=np.float64)

    everyday_mask = predicted_labels == 0
    exotic_mask = predicted_labels == 1

    if everyday_mask.any():
        predictions[everyday_mask] = regressor_everyday.predict(X_test.loc[everyday_mask])
    if exotic_mask.any():
        predictions[exotic_mask] = regressor_exotic.predict(X_test.loc[exotic_mask])

    rmse = np.sqrt(mean_squared_error(y_test, predictions))
    mae = mean_absolute_error(y_test, predictions)
    mape = mean_absolute_percentage_error(y_test, predictions)
    r2 = r2_score(y_test, predictions)

    print("--- Full Two-Stage Routed System Results ---")
    print(f"Routed to everyday regressor: {everyday_mask.sum():,}")
    print(f"Routed to exotic regressor:   {exotic_mask.sum():,}")
    print(f"RMSE:  {rmse:.4f}")
    print(f"MAE:   {mae:.4f}")
    print(f"MAPE:  {mape:.4f} ({mape * 100:.2f}%)")
    print(f"R2:    {r2:.4f}\n")
    return rmse, mae, mape, r2


def main():
    X_train, y_train, X_test, y_test = load_and_split_data()
    preprocessor_tree, preprocessor_linear, _, _, _ = build_pipelines(X_train)

    y_train_classifier = (y_train >= HIGH_VALUE_THRESHOLD).astype(int)

    lgbm_classifier_pipeline = Pipeline(
        steps=[
            ("preprocessor", preprocessor_tree),
            (
                "model",
                LGBMClassifier(
                    class_weight="balanced",
                    n_jobs=-1,
                    random_state=RANDOM_STATE,
                    verbose=-1,
                ),
            ),
        ]
    )
    logistic_classifier_pipeline = Pipeline(
        steps=[
            ("preprocessor", preprocessor_linear),
            (
                "model",
                LogisticRegression(
                    penalty="elasticnet",
                    solver="saga",
                    l1_ratio=0.5,
                    n_jobs=-1,
                    max_iter=200,
                    class_weight="balanced",
                    random_state=RANDOM_STATE,
                ),
            ),
        ]
    )
    classifier = VotingClassifier(
        estimators=[
            ("lgbm", lgbm_classifier_pipeline),
            ("logistic", logistic_classifier_pipeline),
        ],
        voting="soft",
    )
    classifier = tune_and_train_classifier(classifier, X_train, y_train_classifier)
    evaluate_classifier(classifier, X_test, y_test)
    joblib.dump(classifier, OUTPUT_DIR / "Stage1_High_Value_Router.joblib")

    everyday_mask = y_train < HIGH_VALUE_THRESHOLD
    exotic_mask = y_train >= HIGH_VALUE_THRESHOLD
    X_train_everyday = X_train.loc[everyday_mask].reset_index(drop=True)
    y_train_everyday = y_train.loc[everyday_mask].reset_index(drop=True)
    X_train_exotic = X_train.loc[exotic_mask].reset_index(drop=True)
    y_train_exotic = y_train.loc[exotic_mask].reset_index(drop=True)

    print(f"\nEveryday training rows: {X_train_everyday.shape[0]:,}")
    print(f"Exotic training rows:   {X_train_exotic.shape[0]:,}")

    everyday_lgbm = Pipeline(
        steps=[
            ("preprocessor", preprocessor_tree),
            (
                "model",
                TransformedTargetRegressor(
                    regressor=LGBMRegressor(n_jobs=-1, random_state=RANDOM_STATE, verbose=-1),
                    func=np.log1p,
                    inverse_func=np.expm1,
                ),
            ),
        ]
    )
    everyday_lgbm_grid = {
        "model__regressor__num_leaves": [31, 63, 127],
        "model__regressor__learning_rate": [0.03, 0.05, 0.1],
        "model__regressor__n_estimators": [300, 600, 900],
        "model__regressor__max_depth": [-1, 10, 20],
    }
    everyday_lgbm = tune_and_train_regressor(
        everyday_lgbm,
        everyday_lgbm_grid,
        X_train_everyday,
        y_train_everyday,
        "Everyday LightGBM Regressor",
    )
    joblib.dump(everyday_lgbm, OUTPUT_DIR / "Stage2_Everyday_LightGBM.joblib")

    everyday_ridge = Pipeline(
        steps=[
            ("preprocessor", preprocessor_linear),
            (
                "model",
                TransformedTargetRegressor(
                    regressor=Ridge(solver="auto", random_state=RANDOM_STATE),
                    func=np.log1p,
                    inverse_func=np.expm1,
                ),
            ),
        ]
    )
    everyday_ridge_grid = {
        "model__regressor__alpha": [0.1, 1.0, 10.0, 50.0, 100.0],
    }
    everyday_ridge = tune_and_train_regressor(
        everyday_ridge,
        everyday_ridge_grid,
        X_train_everyday,
        y_train_everyday,
        "Everyday Ridge Baseline",
    )
    joblib.dump(everyday_ridge, OUTPUT_DIR / "Stage2_Everyday_Ridge.joblib")

    exotic_rf = Pipeline(
        steps=[
            ("preprocessor", preprocessor_tree),
            (
                "model",
                TransformedTargetRegressor(
                    regressor=RandomForestRegressor(
                        n_jobs=-1,
                        random_state=RANDOM_STATE,
                        bootstrap=True,
                    ),
                    func=np.log1p,
                    inverse_func=np.expm1,
                ),
            ),
        ]
    )
    exotic_rf_grid = {
        "model__regressor__n_estimators": [200, 400, 600],
        "model__regressor__max_depth": [10, 20, None],
        "model__regressor__min_samples_leaf": [2, 5, 10],
        "model__regressor__max_features": ["sqrt", 0.5, 1.0],
    }
    exotic_rf = tune_and_train_regressor(
        exotic_rf,
        exotic_rf_grid,
        X_train_exotic,
        y_train_exotic,
        "Exotic Random Forest Regressor",
        sample_size=50_000,
    )
    joblib.dump(exotic_rf, OUTPUT_DIR / "Stage3_Exotic_RandomForest.joblib")

    exotic_huber = Pipeline(
        steps=[
            ("preprocessor", preprocessor_linear),
            (
                "model",
                TransformedTargetRegressor(
                    regressor=HuberRegressor(max_iter=300),
                    func=np.log1p,
                    inverse_func=np.expm1,
                ),
            ),
        ]
    )
    exotic_huber_grid = {
        "model__regressor__epsilon": [1.1, 1.35, 1.75, 2.0],
        "model__regressor__alpha": [0.0001, 0.001, 0.01],
    }
    exotic_huber = tune_and_train_regressor(
        exotic_huber,
        exotic_huber_grid,
        X_train_exotic,
        y_train_exotic,
        "Exotic Huber Baseline",
        sample_size=50_000,
    )
    joblib.dump(exotic_huber, OUTPUT_DIR / "Stage3_Exotic_Huber.joblib")

    evaluate_regressor(
        "Everyday LightGBM on true everyday test rows",
        everyday_lgbm,
        X_test.loc[y_test < HIGH_VALUE_THRESHOLD],
        y_test.loc[y_test < HIGH_VALUE_THRESHOLD],
    )
    evaluate_regressor(
        "Exotic Random Forest on true exotic test rows",
        exotic_rf,
        X_test.loc[y_test >= HIGH_VALUE_THRESHOLD],
        y_test.loc[y_test >= HIGH_VALUE_THRESHOLD],
    )
    evaluate_full_pipeline(classifier, everyday_lgbm, exotic_rf, X_test, y_test)

    print(f"\nAll routing models saved successfully to {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
