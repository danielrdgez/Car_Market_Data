import sqlite3
import pandas as pd
import numpy as np
import joblib
from pathlib import Path
import warnings

from sklearn.model_selection import RandomizedSearchCV, GroupShuffleSplit
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from category_encoders import TargetEncoder
from lightgbm import LGBMRegressor

warnings.filterwarnings('ignore')

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "CAR_DATA_OUTPUT" / "CAR_DATA_CLEANED.db"
OUTPUT_DIR = BASE_DIR / "MODELS_OUTPUT"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def load_and_split_data():
    """Load data and split 80/20 grouped by Make, Model, Year."""
    print(f"Loading data from {DB_PATH}")
    # Connect with sqlite
    conn = sqlite3.connect(DB_PATH)

    # Drop specified columns
    cols_to_drop = [
        'vin', 'year', 'details', 'title', 'date', 'location', 'currentBid', 'bids',
        'vehicleTitleDesc', 'img', 'nhtsa_Trim2', 'nhtsa_safety_ratings_count',
        'nhtsa_overall_rating', 'nhtsa_front_crash_rating', 'nhtsa_recall_components',
        'nhtsa_latest_recall_date', 'nhtsa_complaint_injuries', 'nhtsa_complain_deaths',
        'nhtsa_complaint_crash_related', 'nhtsa_complaint_fire_related', 'nhtsa_common_complaint_areas',
        'nhtsa_ModelID', 'nhtsa_MakeID', 'nhtsa_BasePrice', 'loaddate',
        'nhtsa_AdditionalErrorText', 'nhtsa_ManufacturerId', 'locationCode', 'nhtsa_OtherEngineInfo', 'nhtsa_DisplacementCC',
        'nhtsa_DisplacementCI', 'countryCode', 'nhtsa_complaint_deaths'
    ]

    # Get all available columns from both tables to construct a selective query
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info('listings')")
    listings_cols = [row[1] for row in cursor.fetchall()]
    cursor.execute("PRAGMA table_info('nhtsa_enrichment')")
    nhtsa_cols = [row[1] for row in cursor.fetchall() if row[1] != 'vin']

    all_cols = listings_cols + nhtsa_cols
    selected_cols = [c for c in all_cols if c not in cols_to_drop]

    query = f"""
    SELECT {', '.join(selected_cols)} 
    FROM listings 
    JOIN nhtsa_enrichment USING(vin)
    """

    # We load chunks iteratively if too large, but with 80GB RAM, pandas can handle 1.6M rows comfortably
    df = pd.read_sql_query(query, conn)
    conn.close()

    # Downcast floating points and integers to save memory
    float_cols = df.select_dtypes(include=['float64']).columns
    int_cols = df.select_dtypes(include=['int64']).columns
    df[float_cols] = df[float_cols].astype('float32')
    df[int_cols] = df[int_cols].astype('int32')

    # Create composite string for grouping
    # Assuming columns are nhtsa_Make, nhtsa_Model, nhtsa_ModelYear
    make_col = 'nhtsa_Make' if 'nhtsa_Make' in df.columns else 'Make'
    model_col = 'nhtsa_Model' if 'nhtsa_Model' in df.columns else 'Model'
    year_col = 'nhtsa_ModelYear' if 'nhtsa_ModelYear' in df.columns else 'Year'
    target_col = 'price' if 'price' in df.columns else 'Price'

    df['group_key'] = df[make_col].astype(str) + "_" + df[model_col].astype(str) + "_" + df[year_col].astype(str)

    # Custom 80/20 train/test grouped split ensuring small groups stay in train set
    counts = df['group_key'].value_counts()

    # Define a threshold for "small group" that can't be split properly (e.g., less than 5 rows)
    small_groups = counts[counts < 5].index

    # Split the dataset into eligible for test and mandatory train
    mandatory_train_df = df[df['group_key'].isin(small_groups)]
    splittable_df = df[~df['group_key'].isin(small_groups)]

    # Stratified/Grouped split on the splittable portion
    gss = GroupShuffleSplit(n_splits=1, test_size=0.2, random_state=42)
    train_idx, test_idx = next(gss.split(splittable_df, groups=splittable_df['group_key']))

    train_split_df = splittable_df.iloc[train_idx]
    test_df = splittable_df.iloc[test_idx]

    # Recombine train
    train_df = pd.concat([mandatory_train_df, train_split_df], axis=0).reset_index(drop=True)
    test_df = test_df.reset_index(drop=True)

    print(f"Train size: {train_df.shape[0]} | Test size: {test_df.shape[0]}")

    # Separate features and target
    X_train = train_df.drop(columns=[target_col, 'group_key'])
    y_train = train_df[target_col]

    X_test = test_df.drop(columns=[target_col, 'group_key'])
    y_test = test_df[target_col]

    return X_train, y_train, X_test, y_test

def build_pipelines(X_train):
    """Constructs preprocessing pipelines with Target Encoding for high-cardinality features."""
    numeric_features = X_train.select_dtypes(include=[np.number]).columns.tolist()
    categorical_features = X_train.select_dtypes(exclude=[np.number]).columns.tolist()

    low_cardinality_threshold = 50
    low_card_cols = [c for c in categorical_features if X_train[c].nunique() <= low_cardinality_threshold]
    high_card_cols = [c for c in categorical_features if X_train[c].nunique() > low_cardinality_threshold]

    numeric_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='median')),
        ('scaler', StandardScaler())
    ])

    categorical_transformer_low = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='constant', fill_value='UNKNOWN')),
        ('onehot', OneHotEncoder(handle_unknown='ignore', sparse_output=False))
    ])

    categorical_transformer_high = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='constant', fill_value='UNKNOWN')),
        ('target', TargetEncoder())
    ])

    preprocessor_with_target = ColumnTransformer(
        transformers=[
            ('num', numeric_transformer, numeric_features),
            ('cat_low', categorical_transformer_low, low_card_cols),
            ('cat_high', categorical_transformer_high, high_card_cols)
        ],
        remainder='passthrough'
    )

    preprocessor_linear = ColumnTransformer(
        transformers=[
            ('num', numeric_transformer, numeric_features),
            ('cat_low', categorical_transformer_low, low_card_cols),
            ('cat_high', categorical_transformer_high, high_card_cols)
        ],
        remainder='passthrough'
    )

    return preprocessor_with_target, preprocessor_linear, numeric_features, low_card_cols, high_card_cols

def evaluate_model(name, model, X_test, y_test):
    """Evaluate pipeline on testing set."""
    preds = model.predict(X_test)
    rmse = np.sqrt(mean_squared_error(y_test, preds))
    mae = mean_absolute_error(y_test, preds)
    r2 = r2_score(y_test, preds)

    print(f"--- {name} Results ---")
    print(f"RMSE:  {rmse:.4f}")
    print(f"MAE:   {mae:.4f}")
    print(f"R2:    {r2:.4f}\n")
    return rmse, mae, r2

def tune_and_train(pipeline, param_grid, X_train, y_train, X_test, y_test, model_name):
    """Subsample training data for hyperparameter tuning, then train on full data."""
    subsample_size = min(100000, X_train.shape[0])
    sample_idx = np.random.choice(X_train.shape[0], size=subsample_size, replace=False)
    X_tune = X_train.iloc[sample_idx]
    y_tune = y_train.iloc[sample_idx]

    print(f"\nTuning {model_name} on {subsample_size:,} sample rows...")
    search = RandomizedSearchCV(
        pipeline,
        param_distributions=param_grid,
        n_iter=5,
        cv=3,
        scoring='neg_root_mean_squared_error',
        n_jobs=-1,
        random_state=42,
        verbose=1
    )

    search.fit(X_tune, y_tune)
    print(f"Best parameters for {model_name}: {search.best_params_}")

    print(f"Training {model_name} on full dataset ({X_train.shape[0]:,} rows)...")
    best_model = search.best_estimator_
    best_model.fit(X_train, y_train)

    evaluate_model(model_name, best_model, X_test, y_test)
    return best_model


def main():
    X_train, y_train, X_test, y_test = load_and_split_data()
    preprocessor_with_target, preprocessor_linear, numeric_features, low_card_cols, high_card_cols = build_pipelines(X_train)

    # 1. Ridge Regression (Fast Linear Model with regularization)
    print("Training Ridge Regression...")
    ridge_pipeline = Pipeline(steps=[
        ('preprocessor', preprocessor_linear),
        ('model', Ridge(solver='auto', random_state=42, alpha=1.0))
    ])
    ridge_pipeline.fit(X_train, y_train)
    evaluate_model("Ridge Regression", ridge_pipeline, X_test, y_test)
    joblib.dump(ridge_pipeline, OUTPUT_DIR / 'Ridge_Regression.joblib')

    # 2. LightGBM (Highly scalable tree model)
    print("\nTuning and Training LightGBM...")
    lgbm_pipeline = Pipeline(steps=[
        ('preprocessor', preprocessor_with_target),
        ('model', LGBMRegressor(random_state=42, n_jobs=-1, verbose=-1))
    ])
    lgbm_param_grid = {
        'model__num_leaves': [31, 50, 127],
        'model__learning_rate': [0.01, 0.05, 0.1],
        'model__n_estimators': [100, 300, 500],
        'model__max_depth': [5, 10, 20, -1]
    }
    lgbm_model = tune_and_train(lgbm_pipeline, lgbm_param_grid, X_train, y_train, X_test, y_test, "LightGBM")
    joblib.dump(lgbm_model, OUTPUT_DIR / 'LightGBM_Tuned.joblib')

    # 3. HistGradientBoosting (Also scalable)
    print("\nTuning and Training HistGradientBoosting...")
    hgb_pipeline = Pipeline(steps=[
        ('preprocessor', preprocessor_with_target),
        ('model', HistGradientBoostingRegressor(random_state=42))
    ])
    hgb_param_grid = {
        'model__learning_rate': [0.01, 0.05, 0.1],
        'model__max_iter': [100, 300, 500],
        'model__max_depth': [5, 10, 15, None]
    }
    hgb_model = tune_and_train(hgb_pipeline, hgb_param_grid, X_train, y_train, X_test, y_test, "HistGradientBoosting")
    joblib.dump(hgb_model, OUTPUT_DIR / 'HistGradientBoosting_Tuned.joblib')

    print(f"\n✓ All models saved successfully to {OUTPUT_DIR}")

if __name__ == "__main__":
    main()

