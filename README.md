# College Football Predictions

## Overview
This project aims to predict college football game results during conference play (Week 4 and beyond). Predictions are likely to reflect real results when the deviation from the spread is greater than 3 points. Predictions are made weekly using the `make_predictions.py` script, which leverages helper files and data files.

## Repository Structure
- **`make_predictions.py`**: Main script for making weekly predictions.
- **Helper Files**:
  - `select_features.py`: Selects the relevant features for the model.
  - `update_game_data.py`: Updates game data used for predictions.
- **Data Files**:
  - `XGBoost_for_spread_cfb.dat`: Pre-trained XGBoost model (included for completeness, but not used--I found that the neural net was more accurate on its own in a validation set).
  - `cfb_feature_normalizations.dat`: Normalization parameters for features.
  - `features_for_cfb_model.dat`: Feature set used by the model.
  - `neural_net_for_spread_cfb.dat`: Pre-trained neural network model.
- **Training File**
  - `College Football Model Fitting.ipynb`: Shows the process of training the models.

## Usage

    Making Predictions: Run make_predictions.py to generate predictions for the upcoming week's games. The script will output the predicted spreads and the deviation from the actual spreads. This uses live dates so works after week 3 of a season. 



