import pandas as pd
import numpy as np
from joblib import load

from featurestore import FeatureStore

class Prediction(object):

    def __init__(self, data_to_predict_json):
        # Load Feature Store (In a real world, this would be a service)
        self.fs = FeatureStore()

        # Load Model (Using Model 1)
        self.estimator = load('gbdt_delovery_time_v1.joblib')

        print('[PREDICTION] Loading Prediction Data {0}'.format(data_to_predict_json))
        self.data = pd.read_json(data_to_predict_json, lines = True)
        self.data = self.data.replace('NA',np.nan)

        # Change data types
        self.data['estimated_store_to_consumer_driving_duration'] = \
            self.data['estimated_store_to_consumer_driving_duration'].astype('float64')
        self.data['market_id'] = self.data['market_id'].astype('float64')
        self.data['order_protocol'] = self.data['order_protocol'].astype('float64')
        self.data['total_busy_dashers'] = self.data['total_busy_dashers'].astype('float64')
        self.data['total_onshift_dashers'] = self.data['total_onshift_dashers'].astype('float64')
        self.data['total_outstanding_orders'] = self.data['total_outstanding_orders'].astype('float64')


        print('[PREDICTION] Prediction Data Shape {0}'.format(self.data.shape))
        print('[PREDICTION] Prediction Data Cols {0}'.format(self.data.columns))
        print(self.data.dtypes)

        # Keep a copy of delivery Ids
        self.delivery_id = self.data['delivery_id'].values

        self.data = self._feature_transformation(self.data)
        self.data = self._handle_missing_data(self.data)

        # Drop unnecessary columns
        drop_cols = ['created_at', 'store_primary_category', 'delivery_id', 'platform']
        self.data.drop(drop_cols, axis=1, inplace=True)

        print('[PREDICTION] Prediction Data Shape (After Feature transformation) {0}'.format(self.data.shape))
        print('[PREDICTION] Prediction Data Cols (After Feature transformation) {0}'.format(self.data.columns))
        print(self.data.dtypes)

        # Debug
        #self.data.to_csv('data_to_predict_transformed.csv')

    def _feature_transformation(self, data_test):
        # Change data type to date and create a new col: delivery_time_secs which would be the model target
        # Convert UTC to PST just to understand the data more in local time
        data_test['created_at'] = pd.to_datetime(data_test['created_at']).dt.tz_localize('utc').dt.tz_convert('US/Pacific')

        # Create additional date-time features based on created_at
        data_test['hour'] = data_test['created_at'].dt.hour
        data_test['day'] = data_test['created_at'].dt.dayofweek
        # Different Buckets: Early morn, breakfast, lunch, snack, dinner etc
        # 0-6 means: starting from 00:00 to 05:59
        data_test['hour_0_6'] = data_test['hour'].map(lambda x: 1 if 0 <= x < 6 else 0)
        data_test['hour_6_11'] = data_test['hour'].map(lambda x: 1 if 6 <= x < 11 else 0)
        data_test['hour_11_15'] = data_test['hour'].map(lambda x: 1 if 11 <= x < 15 else 0)
        data_test['hour_15_18'] = data_test['hour'].map(lambda x: 1 if 15 <= x < 18 else 0)
        data_test['hour_18_23'] = data_test['hour'].map(lambda x: 1 if 18 <= x < 23 else 0)

        data_test['avg_price_item'] = data_test['subtotal'] / data_test['total_items']
        data_test['avg_price_distinct_item'] = data_test['subtotal'] / data_test['num_distinct_items']

        # Add aggregate features
        # Aggregate features from Model-2
        #data_test = self.fs.add_aggregate_features_delivery_time_secs(data_test)
        data_test = self.fs.add_aggregate_features_estimated_store_to_consumer_driving_duration(data_test)

        return data_test

    def _handle_missing_data(self, data_test):
        # Create a market: unknown (id=99)
        data_test['market_id'] = np.where(data_test['market_id'].isna(), 99, data_test['market_id'])

        # Set to "other" wherever NA
        data_test['store_primary_category'] = np.where(data_test['store_primary_category'].isna(), 'other',
                                                       data_test['store_primary_category'])
        data_test['store_primary_category'] = data_test['store_primary_category'].astype('category')
        data_test['store_primary_category_cat'] = data_test['store_primary_category'].cat.codes

        # Create a order_protocol: unknown (id=99)
        data_test['order_protocol'] = np.where(data_test['order_protocol'].isna(), 99, data_test['order_protocol'])

        # Fill with zero
        data_test[['total_onshift_dashers', 'total_busy_dashers', 'total_outstanding_orders']] = \
        data_test[['total_onshift_dashers', 'total_busy_dashers', 'total_outstanding_orders']].fillna(0)


        return data_test

    def predict(self):
        X_test = self.data.values

        #Debug
        #print(np.where(np.isnan(X_test)))
        y_hat = self.estimator.predict(X_test)
        pred_df = pd.DataFrame({'delivery_id':self.delivery_id, 'predicted_delivery_seconds':np.exp(y_hat)})
        pred_df.to_csv('delivery_time_sec_predictions.csv', sep='\t', index=False)

        print('\n### Quantiles of Predictions')
        quantiles = [i/100 for i in range(100)]
        for q in quantiles:
            print('Quantile {0} {1}'.format(q, np.quantile(np.exp(y_hat), q)))
        print('Min:', np.min(np.exp(y_hat)))
        print('Max:', np.max(np.exp(y_hat)))
        print('Avg:', np.mean(np.exp(y_hat)))
