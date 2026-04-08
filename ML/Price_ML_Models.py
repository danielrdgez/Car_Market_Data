import sqlite3
import pandas as pd
import numpy as np
import os
import joblib
from pathlib import Path
import warnings

from sklearn.model_selection import RandomizedSearchCV, GroupShuffleSplit
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder, OrdinalEncoder
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LinearRegression, ElasticNet
from sklearn.ensemble import RandomForestRegressor, HistGradientBoostingRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

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
    """Constructs preprocessing pipelines for imputed and non-imputed models."""
    # Identify col types
    numeric_features = X_train.select_dtypes(include=[np.number]).columns.tolist()
    categorical_features = X_train.select_dtypes(exclude=[np.number]).columns.tolist()

    # Calculate mask for HGBR based on max cardinality of 255
    hgbr_categorical_mask = [False] * len(numeric_features)
    for cat_col in categorical_features:
        if X_train[cat_col].nunique(dropna=False) <= 255:
            hgbr_categorical_mask.append(True)
        else:
            hgbr_categorical_mask.append(False)

    # Imputation Strategy for standard models
    numeric_transformer_impute = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='median')),
        ('scaler', StandardScaler())
    ])

    # Strategy for HGBR (No Imputation, handles NaN)
    numeric_transformer_no_impute = Pipeline(steps=[
        ('scaler', StandardScaler())
    ])

    categorical_transformer_ohe = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='constant', fill_value='UNKNOWN')),
        ('onehot', OneHotEncoder(handle_unknown='ignore', sparse_output=True))
    ])

    categorical_transformer_ordinal = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='constant', fill_value='UNKNOWN')),
        ('ordinal', OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value=-1))
    ])

    preprocessor_linear = ColumnTransformer(
        transformers=[
            ('num', numeric_transformer_impute, numeric_features),
            ('cat', categorical_transformer_ohe, categorical_features)
        ],
        remainder='passthrough'
    )

    preprocessor_tree = ColumnTransformer(
        transformers=[
            ('num', numeric_transformer_impute, numeric_features),
            ('cat', categorical_transformer_ohe, categorical_features)
        ],
        remainder='passthrough'
    )

    preprocessor_hgbr = ColumnTransformer(
        transformers=[
            ('num', numeric_transformer_no_impute, numeric_features),
            ('cat', categorical_transformer_ordinal, categorical_features)
        ],
        remainder='passthrough'
    )

    return preprocessor_linear, preprocessor_tree, preprocessor_hgbr, len(numeric_features), hgbr_categorical_mask

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

def main():
    X_train, y_train, X_test, y_test = load_and_split_data()
    preprocessor_linear, preprocessor_tree, preprocessor_hgbr, num_len, hgbr_categorical_mask = build_pipelines(X_train)

    # 4. Hist Gradient Boosting
    print("Tuning and Training Hist Gradient Boosting...")
    
    hgb_net = Pipeline(steps=[
        ('preprocessor', preprocessor_hgbr),
        ('model', HistGradientBoostingRegressor(random_state=42, categorical_features=hgbr_categorical_mask))
    ])
    hgb_param_grid = {
        'model__learning_rate': [0.01, 0.05, 0.1, 0.2],
        'model__max_iter': [100, 250, 500, 1000],
        'model__max_depth': [3, 5, 9, 15, None]
    }
    hgb_search = RandomizedSearchCV(hgb_net, hgb_param_grid, n_iter=15, cv=3, scoring='neg_root_mean_squared_error', n_jobs=-1, random_state=42, verbose=1)
    hgb_search.fit(X_train, y_train)
    print(f"Best HGBR params: {hgb_search.best_params_}")
    evaluate_model("Hist Gradient Boosting", hgb_search.best_estimator_, X_test, y_test)
    joblib.dump(hgb_search.best_estimator_, OUTPUT_DIR / 'Hist_Gradient_Boosting_Tuned.joblib')

    # 3. Random Forest
    print("Tuning and Training Random Forest...")
    rf_net = Pipeline(steps=[
        ('preprocessor', preprocessor_tree),
        ('model', RandomForestRegressor(random_state=42, n_jobs=-1))
    ])
    rf_param_grid = {
        'model__n_estimators': [100, 200, 300],
        'model__max_depth': [10, 20, 30, None],
        'model__min_samples_split': [2, 5, 10]
    }
    # Using 5 iterations to save time, exhaustion balanced with computational limits
    rf_search = RandomizedSearchCV(rf_net, rf_param_grid, n_iter=5, cv=3, scoring='neg_root_mean_squared_error', n_jobs=-1, random_state=42, verbose=1)
    rf_search.fit(X_train, y_train)
    print(f"Best Random Forest params: {rf_search.best_params_}")
    evaluate_model("Random Forest", rf_search.best_estimator_, X_test, y_test)
    joblib.dump(rf_search.best_estimator_, OUTPUT_DIR / 'Random_Forest_Tuned.joblib')


    # 1. Multiple Linear Regression (Interpretability)
    print("Training Multiple Linear Regression...")
    ml_reg = Pipeline(steps=[
        ('preprocessor', preprocessor_linear),
        ('model', LinearRegression(n_jobs=-1))
    ])
    ml_reg.fit(X_train, y_train)
    evaluate_model("Multiple Linear Regression", ml_reg, X_test, y_test)
    joblib.dump(ml_reg, OUTPUT_DIR / 'Multiple_Linear_Regression.joblib')

    # 2. Elastic Net (Regularized MLR)
    print("Tuning and Training Elastic Net...")
    ela_net = Pipeline(steps=[
        ('preprocessor', preprocessor_linear),
        ('model', ElasticNet(random_state=42, max_iter=2000))
    ])
    ela_param_grid = {
        'model__alpha': [0.1, 1.0, 10.0, 100.0],
        'model__l1_ratio': [0.1, 0.5, 0.7, 0.9, 0.99]
    }
    ela_search = RandomizedSearchCV(ela_net, ela_param_grid, n_iter=10, cv=3, scoring='neg_root_mean_squared_error', n_jobs=-1, random_state=42, verbose=1)
    ela_search.fit(X_train, y_train)
    print(f"Best Elastic Net params: {ela_search.best_params_}")
    evaluate_model("Elastic Net", ela_search.best_estimator_, X_test, y_test)
    joblib.dump(ela_search.best_estimator_, OUTPUT_DIR / 'Elastic_Net_Tuned.joblib')

    print(f"All models saved successfully to {OUTPUT_DIR}")

if __name__ == "__main__":
    main()

