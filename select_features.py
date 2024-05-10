from sklearn import preprocessing
from fastai.tabular import *
from fastai.tabular.all import *
from sklearn.linear_model import Lasso
from sklearn.linear_model import LassoCV
from sklearn.feature_selection import SelectFromModel
from bayes_opt import BayesianOptimization

def select_features(df_z_scaled, excluded=[],cat_features=[], threshold=0.8):
    '''For feature selection. Returns an array of features to be used in the model, selected using the LASSO method.
    Takes as arguments a normalized data frame. Rejects features listed under excluded and takes as categorical
    those listed in cat_features. Rejection of correlated features occurs at the given threshold.'''

    
    cont_features = [c for c in df.columns.to_list() if c not in cat_features and c not in excluded]

    df_feature_sel = df_z_scaled.copy()

    first = True
    count = 0
    correlated_points = {}

    le = LabelEncoder()
    # These encoded labels aren't actually used, but they're needed for proper indexing of the correlation matrix
    df_feature_sel['home_conference'] = le.fit_transform(df_feature_sel['home_conference'])
    df_feature_sel['away_conference'] = le.fit_transform(df_feature_sel['away_conference'])
    df_feature_sel['home_team'] = le.fit_transform(df_feature_sel['home_team'])
    df_feature_sel['away_team'] = le.fit_transform(df_feature_sel['away_team'])

    while len(correlated_points.keys()) > 2 or first:
        first = False
        margin_idx = df_feature_sel.columns.get_loc("margin")
        filter_data = df_feature_sel.corr().to_numpy()
        correlated_points = {}

        for i, row in enumerate(filter_data):
            for j, data in enumerate(row):
                if abs(data) > threshold and i != j and i != margin_idx and j != margin_idx:
                    correlated_points[(i,j)] = abs(filter_data[i][margin_idx])

        i,j = min(correlated_points, key=correlated_points.get)
        
        print('Dropped ', df_feature_sel.columns[i], ' due to collision with ', df_feature_sel.columns[j])
        print('Correlation ', filter_data[i][j])
        count += 1
        print('Drop count: ', count)

        df_feature_sel = df_feature_sel.iloc[:, [j for j, c in enumerate(df_feature_sel.columns) if j != i]]

    train_df = df_feature_sel.query("2015 < year < 2023")
    cont_features = [c for c in df_feature_sel.columns.to_list() if c not in cat_features and c not in excluded]
    splits = RandomSplitter(valid_pct=0.2)(range_of(train_df))
    to = TabularPandas(train_df, procs=[Categorify, Normalize],
                        y_names="margin",
                        cat_names = cat_features,
                        cont_names = cont_features,
                       splits=splits)

    X_train, y_train = to.train.xs, to.train.ys.values.ravel()
    X_test, y_test = to.valid.xs, to.valid.ys.values.ravel()
    lasso_cv = LassoCV(cv=5, random_state=0).fit(X_train, y_train)
    sfm = SelectFromModel(lasso_cv, prefit=True) 
    selected_feature_indices = np.where(sfm.get_support())[0] 
    selected_features = X_train.columns[selected_feature_indices] 
    coefficients = lasso_cv.coef_ 
    print("Selected Features:", selected_features) 
    print("Feature Coefficients:", coefficients) 
    return selected_features
